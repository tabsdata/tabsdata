//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::VecDeque;

/// The shortest chain of declared transitions from `from` to `to`, `from`
/// excluded: the statuses a test walks a row through to reach `to` past the
/// database's transition guards.
pub fn path<T: Copy + Eq + std::fmt::Debug>(transitions: &[(T, T)], from: T, to: T) -> Vec<T> {
    let mut queue = VecDeque::from([vec![from]]);
    let mut seen = vec![from];
    while let Some(chain) = queue.pop_front() {
        let last = *chain.last().unwrap();
        if last == to {
            return chain.into_iter().skip(1).collect();
        }
        for (_, next) in transitions.iter().filter(|(f, _)| *f == last) {
            if !seen.contains(next) {
                seen.push(*next);
                let mut longer = chain.clone();
                longer.push(*next);
                queue.push_back(longer);
            }
        }
    }
    panic!("no declared transitions lead from {from:?} to {to:?}")
}
