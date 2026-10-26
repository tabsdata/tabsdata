//
// Copyright 2026 Tabsdata Inc.
//

use td_database::access::{AccessMode, InProject, LockInstant, SeesProjectRows, WriteMode};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::FromDao;
use td_objects::Taggable;
use td_objects::and;
use td_objects::dxo::crudl::RequestContext;
use td_objects::dxo::tag::{Tag, TagDB, TagSet, UserTagDB, UserTagInsertDB};
use td_objects::types::basic::{
    TagEntity, TagEntityId, TagEntityType, TagId, TagKey, TagKind, UserTagKey,
};
use td_queries::Queries;

#[td_error]
pub enum TagError {
    #[error("Tag '{0}' is not set")]
    NotFound(UserTagKey) = 0,
}

pub async fn entity_tags<S: SeesProjectRows, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    entity: TagEntity,
) -> Result<Vec<Tag>, TdError> {
    let tags: Vec<TagDB> = conn.scd2().fetch_all(entity.rows()).await?;
    tags.into_iter().map(Tag::from_dao).collect()
}

pub async fn set_user_tag(
    conn: &mut DomainConnection<Apiserver, InProject, impl WriteMode>,
    ctx: &RequestContext,
    entity: TagEntity,
    set: TagSet,
    at: LockInstant,
) -> Result<Tag, TdError> {
    let key: TagKey = set.key.clone().try_into()?;
    let current: Option<TagDB> = conn
        .scd2()
        .fetch_optional(and!(
            &entity.entity_type,
            &entity.entity_id,
            &key,
            TagKind::User
        ))
        .await?;
    let tag_id = match current {
        Some(current) if current.value == set.value => return Tag::from_dao(current),
        Some(current) => current.tag_id,
        None => TagId::default(),
    };
    let version = UserTagInsertDB::new(tag_id, entity, set.key, set.value, ctx.user_id);
    conn.scd2().open(&version, at).await?;
    Ok(Tag {
        id: version.id,
        entity_type: version.entity_type,
        entity_id: version.entity_id,
        kind: TagKind::User,
        key,
        value: version.value,
    })
}

pub async fn delete_user_tag<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S, impl WriteMode>,
    entity: TagEntity,
    key: UserTagKey,
    at: LockInstant,
) -> Result<(), TdError> {
    let closed = conn
        .scd2()
        .close::<UserTagDB>(and!(entity.rows(), &key, UserTagDB::user()), at)
        .await?;
    if closed.rows_affected() == 0 {
        Err(TagError::NotFound(key))?
    }
    Ok(())
}

/// The open user tags of the entities, closed with them at the lock instant.
pub async fn close_user_tags_of<T: Taggable<EntityType = TagEntityType>, S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S, impl WriteMode>,
    entities: &[T],
    at: LockInstant,
) -> Result<(), TdError> {
    if entities.is_empty() {
        return Ok(());
    }
    let entity_ids: Vec<TagEntityId> = entities.iter().map(|id| TagEntityId::of(*id)).collect();
    conn.scd2()
        .chunked()
        .close::<UserTagDB>(&entity_ids, and!(&T::ENTITY_TYPE, UserTagDB::user()), at)
        .await?;
    Ok(())
}
