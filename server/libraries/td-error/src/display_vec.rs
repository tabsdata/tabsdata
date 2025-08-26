//
// Copyright 2025 Tabs Data Inc.
//

use itertools::Itertools;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;

#[macro_export]
macro_rules! display_vec {
    ($($item:expr_2021),* $(,)?) => {
        DisplayVec::new(vec![$($item),*])
    };
}

/// A wrapper around `Vec<T>` that implements `Display` and `Debug` traits so inner types
/// can be printed using `Display` and not `Debug`.
pub struct DisplayVec<T>(Vec<T>);

impl<T> DisplayVec<T> {
    pub fn new(vec: Vec<T>) -> Self {
        Self(vec)
    }
}

impl<T> From<Vec<T>> for DisplayVec<T> {
    fn from(vec: Vec<T>) -> Self {
        Self(vec)
    }
}

impl<T> Deref for DisplayVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Display> Debug for DisplayVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl<T: Display> Display for DisplayVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.iter().join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    #[test]
    fn test_display_vec_macro() {
        let vec = display_vec![1, 2, 3];
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], 1);
        assert_eq!(vec[1], 2);
        assert_eq!(vec[2], 3);
    }

    #[test]
    fn test_display_vec_new() {
        let vec = DisplayVec::new(vec![1, 2, 3]);
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], 1);
        assert_eq!(vec[1], 2);
        assert_eq!(vec[2], 3);
    }

    #[test]
    fn test_display_vec_from() {
        let vec: DisplayVec<_> = vec![1, 2, 3].into();
        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], 1);
        assert_eq!(vec[1], 2);
        assert_eq!(vec[2], 3);
    }

    #[test]
    fn test_display_trait() {
        let vec = DisplayVec::new(vec![1, 2, 3]);
        assert_eq!(format!("{vec}"), "1, 2, 3");
    }

    #[test]
    fn test_debug_trait() {
        let vec = DisplayVec::new(vec![1, 2, 3]);
        assert_eq!(format!("{vec:?}"), "1, 2, 3");
    }

    #[test]
    fn test_deref_trait() {
        let vec = DisplayVec::new(vec![1, 2, 3]);
        let sum: i32 = vec.iter().sum();
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_display_uses_display_trait() {
        struct CustomStruct;

        impl Display for CustomStruct {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "display")
            }
        }

        impl Debug for CustomStruct {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "debug")
            }
        }

        let vec = DisplayVec::new(vec![CustomStruct, CustomStruct]);
        assert_eq!(format!("{vec}"), "display, display");
    }
}
