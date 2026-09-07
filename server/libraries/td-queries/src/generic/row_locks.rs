//
// Copyright 2026 Tabsdata Inc.
//

//! What the locking reads buy, against a live PostgreSQL: `FOR UPDATE` makes a
//! second reader of the same rows **wait**, while a claim makes it **skip**.
//!
//! Primitive level, not service level, on purpose: the same assertion through
//! two concurrent service calls passes whether or not the reads lock, because
//! `tokio::join!` gives no control over the interleaving window.

#[cfg(test)]
mod tests {
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use std::collections::HashSet;
    use std::time::Duration;
    use ta_objects::Dao;
    use td_database::pool::TdPool;
    use td_objects::types::basic::{ProjectId, ProjectName};

    /// How long the second reader is given before we call it blocked.
    const BLOCKED_AFTER: Duration = Duration::from_millis(300);

    /// A read shape over `projects`: the write DAOs there declare no `select`,
    /// and what the lock covers is the row, not the projected columns.
    #[Dao]
    #[dao(table = "projects", ops(select))]
    struct LockedProjectDao {
        #[dao(pk)]
        id: ProjectId,
        name: ProjectName,
    }

    /// `fetch_all_for_update` blocks a second reader of the same rows until the
    /// first transaction ends; the same read without the lock is served
    /// immediately. The second half is what makes the first half mean
    /// something.
    #[td_test::test(db)]
    async fn test_fetch_all_for_update_blocks_a_second_reader(db: TdPool) {
        let project = db.seed().test_project().await.unwrap();

        let mut holder = db.begin().await.unwrap();
        let locked: Vec<LockedProjectDao> =
            holder.for_update().fetch_all(&project.id).await.unwrap();
        assert_eq!(locked.len(), 1, "the holder must have locked its row");

        // Reading the same row *without* the lock is served immediately, so the
        // wait asserted below comes from the lock and not from the fixture.
        // Released before the next acquire: the test pool holds two
        // connections, which is exactly what a lock waiter parks.
        {
            let mut reader = db.begin().await.unwrap();
            let unlocked: Vec<LockedProjectDao> =
                tokio::time::timeout(BLOCKED_AFTER, reader.fetch_all(&project.id))
                    .await
                    .expect("an unlocked read must not wait")
                    .unwrap();
            assert_eq!(unlocked.len(), 1);
        }

        let mut waiter = db.begin().await.unwrap();
        let blocked = tokio::time::timeout(
            BLOCKED_AFTER,
            waiter
                .for_update()
                .fetch_all::<LockedProjectDao>(&project.id),
        )
        .await;
        assert!(
            blocked.is_err(),
            "a second FOR UPDATE reader of the same row must wait: {blocked:?}"
        );
    }

    #[td_test::test(db)]
    async fn test_claim_all_in_skips_rows_another_claimer_holds(db: TdPool) {
        let seeded = db
            .seed()
            .project(ProjectName::try_from("claim_target").unwrap())
            .await
            .unwrap();

        let mut holder = db.begin().await.unwrap();
        let claimed: Vec<LockedProjectDao> = holder.for_update().claim_all(&()).await.unwrap();
        let held: HashSet<String> = claimed.iter().map(|p| p.id.to_string()).collect();
        assert!(
            held.contains(&seeded.id.to_string()),
            "the first claimer must take the seeded project"
        );

        // Visible unlocked, so the empty claim below is the skip and not an
        // empty fixture. Dropped before the next acquire: the pool holds two.
        {
            let mut reader = db.begin().await.unwrap();
            let visible: Vec<LockedProjectDao> = reader.fetch_all(&()).await.unwrap();
            assert_eq!(visible.len(), claimed.len());
        }

        let mut second = db.begin().await.unwrap();
        let leftover: Vec<LockedProjectDao> =
            tokio::time::timeout(BLOCKED_AFTER, second.for_update().claim_all(&()))
                .await
                .expect("a claim must skip rather than wait")
                .unwrap();
        assert!(
            leftover.is_empty(),
            "a second claimer must skip every row the first holds, got {} row(s)",
            leftover.len()
        );
    }
}
