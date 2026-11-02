//
// Copyright 2026 Tabsdata Inc.
//

use std::sync::{Mutex, PoisonError};
use td_objects::types::basic::ProjectId;

/// Where the request tick resumes its walk over the projects: just after the
/// last project it finished. A tick that runs out of time is cancelled
/// part-way through the walk, so if every tick started at the same project,
/// the ones at the end would go unticked for as long as ticks run out.
#[derive(Debug, Default)]
pub struct TickCursor(Mutex<Option<ProjectId>>);

impl TickCursor {
    /// `projects` in the order the next walk takes them: sorted, starting
    /// after the last project finished, or at the first when that one is
    /// gone or none was.
    pub fn order(&self, mut projects: Vec<ProjectId>) -> Vec<ProjectId> {
        projects.sort();
        if let Some(last) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            let next = projects.partition_point(|project| project <= last);
            projects.rotate_left(next);
        }
        projects
    }

    pub fn finished(&self, project: &ProjectId) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(*project);
    }

    pub fn last(&self) -> Option<ProjectId> {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize) -> Vec<ProjectId> {
        let mut ids: Vec<ProjectId> = (0..n).map(|_| ProjectId::default()).collect();
        ids.sort();
        ids
    }

    #[test]
    fn test_a_fresh_cursor_walks_the_projects_sorted() {
        let ids = ids(3);
        let shuffled = vec![ids[2], ids[0], ids[1]];
        assert_eq!(TickCursor::default().order(shuffled), ids);
    }

    #[test]
    fn test_the_walk_resumes_after_the_last_project_finished() {
        let ids = ids(4);
        let cursor = TickCursor::default();
        cursor.finished(&ids[1]);
        assert_eq!(
            cursor.order(ids.clone()),
            vec![ids[2], ids[3], ids[0], ids[1]]
        );
        cursor.finished(&ids[3]);
        assert_eq!(cursor.order(ids.clone()), ids);
    }

    #[test]
    fn test_a_finished_project_since_gone_resumes_at_the_next() {
        let ids = ids(4);
        let cursor = TickCursor::default();
        cursor.finished(&ids[1]);
        let left = vec![ids[0], ids[2], ids[3]];
        assert_eq!(cursor.order(left), vec![ids[2], ids[3], ids[0]]);
    }
}
