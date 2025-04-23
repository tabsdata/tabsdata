//
// Copyright 2024 Tabs Data Inc.
//

use crate::users::error::UserError;
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use std::ops::Deref;
use td_database::sql::DbError;
use td_error::TdError;
use td_objects::crudl::{
    assert_one, handle_create_unique_err, handle_sql_err, list_result, list_select, ListRequest,
    ListResult,
};
use td_objects::dlo::{RequestIsAdmin, RequestTime, RequestUserId, UserId, Value};
use td_objects::jwt::jwt_logic::{JwtLogic, TokenResponse};
use td_objects::users::dao::{User, UserBuilder, UserWithNames};
use td_objects::users::dlo::{Password, PasswordProvider, UserPassword, UserPasswordHash};
use td_objects::users::dto::{AuthenticateRequest, PasswordUpdate, UserCreate, UserUpdate};
use td_security::config::PasswordHashingConfig;
use td_security::password;
use td_security::password::{assert_password_policy, create_password_hash};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

pub async fn create_user_authorize(
    Input(req_is_admin): Input<RequestIsAdmin>,
) -> Result<(), TdError> {
    if !req_is_admin.value() {
        return Err(UserError::NotAllowedToCreateUsers)?;
    }
    Ok(())
}

pub async fn user_extract_password<P: PasswordProvider>(
    Input(provider): Input<P>,
) -> Result<Password, TdError> {
    Ok(Password::new(provider.password()))
}

pub async fn user_validate_password(Input(password): Input<Password>) -> Result<(), TdError> {
    let password: &Option<String> = password.deref();
    if let Some(password) = password {
        assert_password_policy(password)?;
    }
    Ok(())
}

pub async fn create_user_build_dao(
    SrvCtx(password_hashing_config): SrvCtx<PasswordHashingConfig>,
    Input(request_time): Input<RequestTime>,
    Input(request_user_id): Input<RequestUserId>,
    Input(user_id): Input<UserId>,
    Input(password): Input<Password>,
    Input(dto): Input<UserCreate>,
) -> Result<User, TdError> {
    let password: &Option<String> = &password;
    let password = match password {
        Some(password) => password,
        None => {
            return Err(UserError::ShouldNotHappen(
                "Password is missing".to_string(),
            ))?
        }
    };

    let user = UserBuilder::default()
        .id(&*user_id)
        .name(dto.name())
        .full_name(dto.full_name())
        .email(dto.email().clone())
        .created_on(&*request_time)
        .created_by_id(&*request_user_id)
        .modified_on(&*request_time)
        .modified_by_id(&*request_user_id)
        .password_hash(create_password_hash(&password_hashing_config, password))
        .password_set_on(&*request_time)
        .password_must_change(true)
        .enabled(dto.enabled().unwrap_or(true))
        .build()
        .map_err(|e| UserError::ShouldNotHappen(e.to_string()))?;
    Ok(user)
}

pub async fn create_user_sql_insert(
    Connection(connection): Connection,
    Input(user): Input<User>,
) -> Result<(), TdError> {
    const INSERT_SQL: &str = r#"
              INSERT INTO users (
                    id,
                    name,
                    full_name,
                    email,
                    created_on,
                    created_by_id,
                    modified_on,
                    modified_by_id,
                    password_hash,
                    password_set_on,
                    password_must_change,
                    enabled
              )
              VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    sqlx::query(INSERT_SQL)
        .bind(user.id())
        .bind(user.name())
        .bind(user.full_name())
        .bind(user.email())
        .bind(user.created_on())
        .bind(user.created_by_id())
        .bind(user.modified_on())
        .bind(user.modified_by_id())
        .bind(user.password_hash())
        .bind(user.password_set_on())
        .bind(user.password_must_change())
        .bind(user.enabled())
        .execute(conn)
        .await
        .map_err(handle_create_unique_err(
            UserError::AlreadyExists,
            DbError::SqlError,
        ))?;
    Ok(())
}

pub async fn read_user_authorize(
    Input(req_is_admin): Input<RequestIsAdmin>,
    Input(req_user_id): Input<RequestUserId>,
    Input(user_id): Input<UserId>,
) -> Result<(), TdError> {
    if !req_is_admin.value() && req_user_id.value() != user_id.value() {
        return Err(UserError::NotAllowedToReadUsers)?;
    }
    Ok(())
}

pub async fn delete_user_validate(
    Input(req_user_id): Input<RequestUserId>,
    Input(user_id): Input<UserId>,
) -> Result<(), TdError> {
    if req_user_id.value() == user_id.value() {
        return Err(UserError::NotAllowedToDeleteThemselves)?;
    }
    Ok(())
}

pub async fn delete_user_sql_delete(
    Connection(connection): Connection,
    Input(user_id): Input<UserId>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const DELETE_SQL: &str = "DELETE FROM users WHERE id = ?1";

    let res = sqlx::query(DELETE_SQL)
        .bind(user_id.as_str())
        .execute(conn)
        .await
        .map_err(handle_sql_err)?;
    assert_one(res)?;
    Ok(())
}

pub async fn list_users_authorize(
    Input(req_is_admin): Input<RequestIsAdmin>,
) -> Result<(), TdError> {
    if !req_is_admin.value() {
        return Err(UserError::NotAllowedToListUsers)?;
    }
    Ok(())
}

pub async fn list_users_sql_select(
    Connection(connection): Connection,
    Input(request): Input<ListRequest<()>>,
) -> Result<ListResult<UserWithNames>, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const LIST_WITH_NAMES_SQL: &str = r#"
            SELECT
                id,
                name,
                full_name,
                email,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,
                password_set_on,
                password_must_change,
                enabled
            FROM users_with_names
        "#;

    let db_data: Vec<UserWithNames> =
        sqlx::query_as(&list_select(request.list_params(), LIST_WITH_NAMES_SQL))
            .persistent(true)
            .fetch_all(conn)
            .await
            .map_err(handle_sql_err)?;
    Ok(list_result(request.list_params().clone(), db_data))
}

pub async fn update_user_validate(Input(dto): Input<UserUpdate>) -> Result<(), TdError> {
    if dto.full_name().is_none()
        && dto.email().is_none()
        && dto.password().is_none()
        && dto.enabled().is_none()
    {
        return Err(UserError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub async fn update_user_validate_password_force_change_as_admin(
    Input(request_user_id): Input<RequestUserId>,
    Input(user_id): Input<UserId>,
    Input(dto): Input<UserUpdate>,
) -> Result<(), TdError> {
    match dto.password() {
        Some(PasswordUpdate::ForceChange { .. }) if request_user_id.value() == user_id.value() => {
            return Err(UserError::CannotForcePasswordChangeToSelf)?;
        }
        _ => {}
    }
    Ok(())
}

pub async fn update_user_validate_password_force_change_as_non_admin(
    Input(dto): Input<UserUpdate>,
) -> Result<(), TdError> {
    match dto.password() {
        Some(PasswordUpdate::ForceChange { .. }) => {
            return Err(UserError::CannotForcePasswordChange)?;
        }
        Some(PasswordUpdate::Change { .. }) => {}
        None => {}
    }
    Ok(())
}

pub async fn update_user_validate_password_change(
    Input(request_user_id): Input<RequestUserId>,
    Input(user_id): Input<UserId>,
    Input(dto): Input<UserUpdate>,
    Input(dao): Input<User>,
) -> Result<(), TdError> {
    match dto.password() {
        Some(PasswordUpdate::Change {
            old_password,
            new_password: _,
        }) if request_user_id.value() == user_id.value() => {
            match PasswordHash::new(dao.password_hash()) {
                // the values encode in the PHC string are used to configure the verifier
                Ok(parsed_hash) => {
                    if Argon2::default()
                        .verify_password(old_password.as_bytes(), &parsed_hash)
                        .is_err()
                    {
                        return Err(UserError::IncorrectOldPassword)?;
                    }
                }
                Err(err) => {
                    return Err(UserError::IncorrectPasswordHash(err.to_string()))?;
                }
            };
        } // user is modifying their own password
        Some(PasswordUpdate::Change { .. }) => {
            return Err(UserError::CannotChangeOtherUserPassword)?;
        }
        Some(PasswordUpdate::ForceChange { .. }) => {}
        None => {}
    }
    Ok(())
}

pub async fn update_user_validate_enabled(
    Input(request_user_id): Input<RequestUserId>,
    Input(user_id): Input<UserId>,
    Input(dto): Input<UserUpdate>,
) -> Result<(), TdError> {
    if dto.enabled().is_some() && request_user_id.value() == user_id.value() {
        return Err(UserError::UserCannotEnableDisableThemselves)?;
    }
    Ok(())
}

pub async fn update_user_build_dao(
    SrvCtx(password_hashing_config): SrvCtx<PasswordHashingConfig>,
    Input(request_user_id): Input<RequestUserId>,
    Input(request_time): Input<RequestTime>,
    Input(dto): Input<UserUpdate>,
    Input(password): Input<Password>,
    Input(dao): Input<User>,
) -> Result<User, TdError> {
    let mut builder = dao.builder();
    dto.full_name()
        .as_ref()
        .map(|value| builder.full_name(value));
    dto.email()
        .as_ref()
        .map(|value| builder.email(value.clone()));

    if let Some(password) = password.value() {
        builder.password_hash(create_password_hash(
            &password_hashing_config,
            password.trim(),
        ));
        builder.password_set_on(&*request_time);
    };

    if let Some(PasswordUpdate::ForceChange { .. }) = dto.password() {
        builder.password_must_change(true);
    }

    dto.enabled().as_ref().map(|value| builder.enabled(*value));
    builder.modified_on(&*request_time);
    builder.modified_by_id(&*request_user_id);
    Ok(builder
        .build()
        .map_err(|e| UserError::ShouldNotHappen(e.to_string()))?)
}

pub async fn update_user_sql_update(
    Connection(connection): Connection,
    Input(user_id): Input<UserId>,
    Input(user): Input<User>,
) -> Result<(), TdError> {
    const UPDATE_SQL: &str = r#"
            UPDATE users SET
                full_name = ?1,
                email = ?2,
                modified_on = ?3,
                modified_by_id = ?4,
                password_hash = ?5,
                password_set_on = ?6,
                password_must_change = ?7,
                enabled = ?8
            WHERE
                id = ?9
        "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let res = sqlx::query(UPDATE_SQL)
        .bind(user.full_name())
        .bind(user.email())
        .bind(user.modified_on())
        .bind(user.modified_by_id())
        .bind(user.password_hash())
        .bind(user.password_set_on())
        .bind(user.password_must_change())
        .bind(user.enabled())
        .bind(user_id.value())
        .execute(conn)
        .await
        .map_err(handle_create_unique_err(
            UserError::AlreadyExists,
            DbError::SqlError,
        ))?;
    assert_one(res)?;

    Ok(())
}

pub async fn auth_user_validate_enabled(Input(user): Input<User>) -> Result<(), TdError> {
    (user.enabled())
        .then_some(())
        .ok_or(UserError::UserNotEnabled.into())
}

pub async fn auth_user_extract_password_hash(
    Input(user): Input<User>,
) -> Result<UserPasswordHash, TdError> {
    Ok(UserPasswordHash::new(user.password_hash()))
}

pub async fn auth_user_authenticate(
    Input(password): Input<UserPassword>,
    Input(password_hash): Input<UserPasswordHash>,
) -> Result<(), TdError> {
    password::verify_password(&password_hash, &password)
        .then_some(())
        .ok_or(UserError::AuthenticationFailed.into())
}

pub async fn auth_user_extract_req_password(
    Input(auth_req): Input<AuthenticateRequest>,
) -> Result<UserPassword, TdError> {
    Ok(UserPassword::new(auth_req.password()))
}

pub async fn auth_user_create_jwt(
    SrvCtx(jwt_logic): SrvCtx<JwtLogic>,
    Input(user_id): Input<UserId>,
) -> Result<TokenResponse, TdError> {
    let token = jwt_logic.authorize_access(&user_id, "user")?;
    Ok(token)
}
