//
// Copyright 2025 Tabsdata Inc.
//

use crate::dxo::crudl::RequestContext;
use crate::dxo::role::Role;
use crate::types::basic::{
    AtTime, Created, Email, FullName, Modified, Password, PasswordChangeTime, PasswordHash,
    PasswordMustChange, UserEnabled, UserId, UserIdName, UserLabel, UserName, UserSystem,
};
use ta_objects::FromDao;
use td_error::TdError;
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;

#[ta_objects::Dao]
#[derive(Eq, PartialEq)]
#[dao(table = "users", ops(select, insert, delete), unique(UserName))]
pub struct UserDB {
    #[dao(pk)]
    pub id: UserId,
    pub name: UserName,
    pub full_name: FullName,
    pub email: Option<Email>,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub password_hash: PasswordHash,
    pub password_set_on: PasswordChangeTime,
    pub password_must_change: PasswordMustChange,
    pub enabled: UserEnabled,
    pub system: UserSystem,
}

#[ta_objects::Dao]
#[dao(view = "users__with_names", unique(UserName))]
pub struct UserDBWithNames {
    #[dao(flatten)]
    pub base: UserDB,
    pub created_by: Created<UserLabel>,
    pub modified_by: Modified<UserLabel>,
}

#[ta_objects::Dto]
pub struct UserCreate {
    pub name: UserName,
    pub full_name: FullName,
    pub email: Option<Email>,
    pub password: Password,
    #[serde(default)]
    pub enabled: UserEnabled,
}

impl UserCreate {
    pub fn to_dao(
        self,
        ctx: &RequestContext,
        password_hashing_config: &PasswordHashingConfig,
    ) -> Result<UserDB, TdError> {
        let password_hash = PasswordHash::try_from(create_password_hash(
            password_hashing_config,
            self.password.trim(),
        ))?;
        Ok(UserDB {
            id: UserId::default(),
            name: self.name,
            full_name: self.full_name,
            email: self.email,
            created_on: ctx.time.into(),
            created_by_id: ctx.user_id.into(),
            modified_on: ctx.time.into(),
            modified_by_id: ctx.user_id.into(),
            password_hash,
            password_set_on: PasswordChangeTime::try_from(&ctx.time)?,
            password_must_change: PasswordMustChange::TRUE,
            enabled: self.enabled,
            system: UserSystem::FALSE,
        })
    }
}

#[ta_objects::Dto]
#[derive(Default)]
#[serde(default)]
pub struct UserUpdate {
    pub full_name: Option<FullName>,
    pub email: Option<Option<Email>>,
    pub password: Option<Password>,
    pub enabled: Option<UserEnabled>,
}

impl UserUpdate {
    /// Seed the update row from the current user, stamp it, and merge the
    /// set fields (a set password re-hashes and forces a change on login).
    pub fn to_dao(
        self,
        user: UserDB,
        ctx: &RequestContext,
        password_hashing_config: &PasswordHashingConfig,
    ) -> Result<UserUpdateDB, TdError> {
        let mut db = UserUpdateDB::from(user);
        db.modified_on = ctx.time.into();
        db.modified_by_id = ctx.user_id.into();
        if let Some(full_name) = self.full_name {
            db.full_name = full_name;
        }
        // Re-wrap as `Some(inner)` so update_values writes the column; the
        // DTO's outer `None` falls through here (unchanged).
        if let Some(email) = self.email {
            db.email = Some(email);
        }
        if let Some(password) = self.password {
            db.password_hash = PasswordHash::try_from(create_password_hash(
                password_hashing_config,
                password.trim(),
            ))?;
            db.password_set_on = PasswordChangeTime::try_from(&ctx.time)?;
            db.password_must_change = PasswordMustChange::TRUE;
        };
        if let Some(enabled) = self.enabled {
            db.enabled = enabled;
        }
        Ok(db)
    }
}

#[ta_objects::Dao]
#[dao(table = "users", ops(update(UserDB)))]
pub struct UserUpdateDB {
    pub full_name: FullName,
    // Tri-state for update_values: outer `None` skips the column (unchanged),
    // `Some(None)` sets it NULL (clear), `Some(Some(v))` sets `v`. Seeded from
    // the current `UserDB`, overridden by the update layer.
    pub email: Option<Option<Email>>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub password_hash: PasswordHash,
    pub password_set_on: PasswordChangeTime,
    pub password_must_change: PasswordMustChange,
    pub enabled: UserEnabled,
}

#[ta_objects::Dto]
#[dto(list(on = UserDBWithNames))]
pub struct UserRead {
    #[dto(list(pagination_by = "+"))]
    pub id: UserId,
    #[dto(list(filter, filter_like, order_by))]
    pub name: UserName,
    #[dto(list(filter, filter_like, order_by))]
    pub full_name: FullName,
    #[dto(list(filter, filter_like))]
    pub email: Option<Email>,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub password_set_on: PasswordChangeTime,
    pub password_must_change: PasswordMustChange,
    pub enabled: UserEnabled,
    pub system: UserSystem,
    pub created_by: Created<UserLabel>,
    pub modified_by: Modified<UserLabel>,
}

impl FromDao for UserRead {
    type Dao = UserDBWithNames;

    fn from_dao(dao: UserDBWithNames) -> Result<Self, TdError> {
        Ok(Self {
            id: dao.base.id,
            name: dao.base.name,
            full_name: dao.base.full_name,
            email: dao.base.email,
            created_on: dao.base.created_on,
            created_by_id: dao.base.created_by_id,
            modified_on: dao.base.modified_on,
            modified_by_id: dao.base.modified_by_id,
            password_set_on: dao.base.password_set_on,
            password_must_change: dao.base.password_must_change,
            enabled: dao.base.enabled,
            system: dao.base.system,
            created_by: dao.created_by,
            modified_by: dao.modified_by,
        })
    }
}

#[ta_objects::Dto]
pub struct UserInfo {
    pub id: UserId,
    pub name: UserName,
    pub full_name: FullName,
    pub email: Option<Email>,
    pub created_on: Created<AtTime>,
    pub created_by_id: Created<UserId>,
    pub modified_on: Modified<AtTime>,
    pub modified_by_id: Modified<UserId>,
    pub password_set_on: PasswordChangeTime,
    pub password_must_change: PasswordMustChange,
    pub enabled: UserEnabled,
    pub system: UserSystem,
    pub created_by: Created<UserLabel>,
    pub modified_by: Modified<UserLabel>,

    pub user_roles: Vec<Role>,
}
impl From<UserDB> for UserUpdateDB {
    fn from(src: UserDB) -> Self {
        UserUpdateDB {
            full_name: src.full_name,
            email: Some(src.email),
            modified_on: src.modified_on,
            modified_by_id: src.modified_by_id,
            password_hash: src.password_hash,
            password_set_on: src.password_set_on,
            password_must_change: src.password_must_change,
            enabled: src.enabled,
        }
    }
}

#[ta_objects::UrlParam]
pub struct UserParam {
    pub user: UserIdName,
}
