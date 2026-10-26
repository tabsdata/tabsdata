//
// Copyright 2026 Tabsdata Inc.
//

//! Latest-version reads, reached through [`Scd2::last`](crate::Scd2::last):
//! every matching timeline's last version, open or closed, so a deleted
//! entity is still found.

use crate::Serves;
use crate::generic::chunked::LastChunked;
use crate::primitives::scd2::select_last;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::SqlCondition;
use ta_objects::{PkDao, Scd2Dao, SelectDao};
use td_database::sea_query::builder::SeaQueryExt;

pub struct Last<E> {
    pub(crate) executor: E,
}

impl<E> Last<E> {
    pub fn chunked(self) -> LastChunked<E> {
        LastChunked {
            executor: self.executor,
        }
    }

    pub async fn fetch_all<D: SelectDao + Scd2Dao + PkDao>(
        self,
        condition: impl SqlCondition<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
    {
        select_last::<D>(condition)
            .order_by_columns(D::order_by())
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<D>)
    }
}

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use td_database::access::LockInstant;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::{CollectionDB, CollectionInsertDB};
    use td_objects::dxo::function::{FunctionDBWithNames, FunctionRegister};
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionLabel, CollectionName, CollectionVersionId, Decorator,
        FunctionName, ReuseFrozen, UserId,
    };
    use td_objects::types::label::Label;

    /// Two versions opened at one instant are the close-out trigger's
    /// zero-length case; the last-version reads see one row, the higher id.
    #[td_test::test(db)]
    async fn test_last_reads_one_row_per_timeline(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let scoped = db.scoped(p.base.project_id);
        let first = scoped
            .seed()
            .collection(&CollectionName::try_from("c").unwrap(), &UserId::admin())
            .await
            .unwrap();
        let create = FunctionRegister {
            child_functions: None,
            name: FunctionName::try_from("f").unwrap(),
            description: "d".try_into().unwrap(),
            bundle_id: BundleId::default(),
            snippet: "s".try_into().unwrap(),
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: None,
            table_types: None,
            parent_tables: None,
            runtime_values: "rv".try_into().unwrap(),
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = scoped.seed().function(&first, &create).await.unwrap();

        let same_instant = LockInstant::asserted(*first.interval.valid_from);
        let renamed = CollectionInsertDB {
            id: CollectionVersionId::default(),
            name: CollectionName::try_from("renamed").unwrap(),
            ..first.base.clone()
        };
        scoped.scd2().open(&renamed, same_instant).await.unwrap();

        let last: Vec<CollectionDB> = scoped.scd2().last().fetch_all(()).await.unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].base.id, renamed.id);

        let rows: Vec<FunctionDBWithNames> = scoped
            .scd2()
            .fetch_all(&function.base.function_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].collection,
            CollectionLabel::new(renamed.name.clone())
        );
        assert_eq!(rows[0].group_id, first.base.group_id);

        scoped
            .scd2()
            .close::<CollectionDB>(
                &first.base.collection_id,
                LockInstant::asserted(*AtTime::now()),
            )
            .await
            .unwrap();
        let rows: Vec<FunctionDBWithNames> = scoped
            .scd2()
            .fetch_all(&function.base.function_id)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].collection,
            CollectionLabel::new(Label::Deleted(first.base.collection_id))
        );
        assert_eq!(rows[0].group_id, first.base.group_id);
        let last: Vec<CollectionDB> = scoped.scd2().last().fetch_all(()).await.unwrap();
        assert_eq!(last.len(), 1);
        assert!(!last[0].interval.valid_to.is_infinity());
    }
}
