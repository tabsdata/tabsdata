//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::crudl::ListParams;
use crate::list::params::{ListQueryParams, Untagged};
use crate::types::tag_selector::TagSelector;
use ta_objects::{FromDao, ListDto, TaggedDao};
use td_error::TdError;

/// A listing of entities that carry tags: its `tag.<key>` filters become the
/// selector, which the read applies at its own grain, and the rest parse as
/// for any listing.
impl<T: ListDto> TryFrom<&ListParams> for ListQueryParams<T, TagSelector>
where
    <T as FromDao>::Dao: TaggedDao,
{
    type Error = TdError;

    fn try_from(value: &ListParams) -> Result<Self, Self::Error> {
        let (tags, columns): (Vec<_>, Vec<_>) = value
            .filter
            .iter()
            .cloned()
            .partition(|f| TagSelector::is_tag_filter(f));
        let tags = TagSelector::parse(&tags)?;
        let ListQueryParams {
            len,
            filter,
            natural_order,
            order,
            pagination,
            tags: Untagged,
        } = ListQueryParams::<T>::try_from(&ListParams {
            filter: columns,
            ..value.clone()
        })?;
        Ok(ListQueryParams {
            len,
            filter,
            natural_order,
            order,
            pagination,
            tags,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::dxo::crudl::ListParams;
    use crate::dxo::table::Table;
    use crate::list::params::ListQueryParams;
    use crate::types::tag_selector::TagSelector;

    fn params(filter: &[&str]) -> ListParams {
        ListParams {
            filter: filter.iter().map(|f| f.to_string()).collect(),
            ..ListParams::default()
        }
    }

    fn tagged(filter: &[&str]) -> Result<ListQueryParams<Table, TagSelector>, td_error::TdError> {
        ListQueryParams::try_from(&params(filter))
    }

    #[test]
    fn test_tag_filters_split_from_the_column_filters() {
        let query = tagged(&["tag.team:eq:a", "name:eq:t", "tag.team:eq:b"]).unwrap();
        assert_eq!(query.tags.0.len(), 1);
        assert_eq!(query.tags.0[0].1.len(), 2);
        assert!(!query.filter.is_empty());
    }

    #[test]
    fn test_a_bad_tag_or_column_filter_fails_the_listing() {
        assert!(tagged(&["tag.team:gt:a"]).is_err());
        assert!(tagged(&["nope:eq:a"]).is_err());
    }

    #[test]
    fn test_an_untagged_parse_has_no_tag_fields() {
        assert!(ListQueryParams::<Table>::try_from(&params(&["tag.team:eq:a"])).is_err());
    }
}
