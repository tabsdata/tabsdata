//
// Copyright 2026 Tabsdata Inc.
//

use super::params::{ListError, ListQueryParams};
use crate::dxo::crudl::{ListParams, ListResponse, ListResponseBuilder};
use std::any::type_name;
use std::collections::HashSet;
use ta_objects::ListDto;
use td_error::TdError;

/// Build ListResponse from list results
pub fn build_list_response<T>(
    list_params: &ListParams,
    query_params: &ListQueryParams<T>,
    mut result: Vec<T>,
) -> Result<ListResponse<T>, TdError>
where
    T: ListDto,
{
    // A repeated cursor value means the pagination column is not unique here:
    // the next page would silently skip the rest of the tie group.
    let mut seen = HashSet::with_capacity(result.len());
    for row in &result {
        if !seen.insert(row.pagination_value()) {
            return Err(ListError::NonUniquePaginationKey(
                type_name::<T>().to_string(),
                row.pagination_value(),
            )
            .into());
        }
    }

    let has_more = result.len() > query_params.len;
    if has_more {
        result.truncate(query_params.len);
    }

    if list_params.previous.is_some() {
        result.reverse();
    }

    let (previous, previous_pagination_id) =
        compute_previous(list_params, query_params, &result, has_more);
    let (next, next_pagination_id) = compute_next(list_params, query_params, &result, has_more);

    let list_response = ListResponseBuilder::default()
        .list_params(list_params.clone())
        .data(result)
        .previous_page(previous, previous_pagination_id)
        .next_page(next, next_pagination_id)
        .build()?;

    Ok(list_response)
}

/// Determine previous info for listing pagination
fn compute_previous<T: ListDto>(
    list_params: &ListParams,
    query_params: &ListQueryParams<T>,
    result: &[T],
    has_more: bool,
) -> (Option<String>, Option<String>) {
    let first = match (&list_params.previous, &list_params.next, result.first()) {
        // Initial request: never a previous page
        (None, None, _) => None,
        // "Next" request: always provide previous cursor (we came from there)
        (None, Some(_), Some(first)) => Some(first),
        // "Previous" request: only if there are more pages further back
        (Some(_), _, Some(first)) if has_more => Some(first),
        _ => None,
    };
    match first {
        None => (None, None),
        Some(first) => {
            let order = query_params
                .order
                .as_ref()
                .unwrap_or(&query_params.natural_order)
                .field();
            (first.order_by_value(order), Some(first.pagination_value()))
        }
    }
}

/// Determine next info for listing pagination
fn compute_next<T: ListDto>(
    list_params: &ListParams,
    query_params: &ListQueryParams<T>,
    result: &[T],
    has_more: bool,
) -> (Option<String>, Option<String>) {
    let last = match (&list_params.previous, result.last()) {
        // "Previous" request: always provide next cursor (we came from there)
        (Some(_), Some(last)) => Some(last),
        // Initial or "Next": only if DB returned more rows than requested
        (_, Some(last)) if has_more => Some(last),
        _ => None,
    };
    match last {
        None => (None, None),
        Some(last) => {
            let order = query_params
                .order
                .as_ref()
                .unwrap_or(&query_params.natural_order)
                .field();
            (last.order_by_value(order), Some(last.pagination_value()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::crudl::ListParamsBuilder;
    use crate::list::params::Order;
    use sea_query::Condition;
    use ta_objects::{Buildable, ListDto};

    #[ta_objects::typed(string)]
    pub struct StringType;

    #[ta_objects::Dao]
    struct TestDao {
        a: StringType,
    }

    #[ta_objects::Dto]
    #[dto(list(on = TestDao))]
    struct TestDto {
        #[dto(list(pagination_by = "+"))]
        a: StringType,
    }

    fn make_dto(val: &str) -> TestDto {
        let dao = TestDao::builder().try_a(val).unwrap().build().unwrap();
        TestDto::from_dao(&dao).unwrap()
    }

    fn make_query_params(len: usize) -> ListQueryParams<TestDto> {
        let (id_col, _) = TestDto::pagination_col();
        ListQueryParams {
            len,
            filter: Condition::all(),
            natural_order: Order::Asc(id_col),
            order: None,
            pagination: None,
        }
    }

    fn make_items(n: usize) -> Vec<TestDto> {
        (0..n).map(|i| make_dto(&format!("item_{i}"))).collect()
    }

    // -----------------------------------------------------------------------
    // Initial requests (no previous, no next)
    // -----------------------------------------------------------------------

    #[test]
    fn test_initial_empty_result() {
        let list_params = ListParamsBuilder::default().len(10usize).build().unwrap();
        let query_params = make_query_params(10);
        let resp = build_list_response(&list_params, &query_params, vec![]).unwrap();
        assert_eq!(resp.data.len(), 0);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_initial_fewer_than_len() {
        let list_params = ListParamsBuilder::default().len(10usize).build().unwrap();
        let query_params = make_query_params(10);
        let result = make_items(5);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 5);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_initial_exact_len_no_next() {
        // N+1 fix: exactly `len` rows means DB had no extra → no next page
        let list_params = ListParamsBuilder::default().len(3usize).build().unwrap();
        let query_params = make_query_params(3);
        let result = make_items(3);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_initial_has_more_truncates() {
        // N+1: len+1 rows means there are more pages; result truncated to len
        let list_params = ListParamsBuilder::default().len(3usize).build().unwrap();
        let query_params = make_query_params(3);
        let result = make_items(4);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.next_pagination_id.is_some());
        assert!(resp.previous.is_none());
    }

    // -----------------------------------------------------------------------
    // "Next" requests (navigating forward)
    // -----------------------------------------------------------------------

    #[test]
    fn test_next_request_no_more() {
        let list_params = ListParamsBuilder::default()
            .len(5usize)
            .next("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(5);
        let result = make_items(3);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_next_request_exact_len_no_more() {
        // N+1 fix: exactly len rows on a "next" request → no more forward
        let list_params = ListParamsBuilder::default()
            .len(3usize)
            .next("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(3);
        let result = make_items(3);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_next_request_has_more() {
        let list_params = ListParamsBuilder::default()
            .len(3usize)
            .next("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(3);
        let result = make_items(4);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_some());
    }

    // -----------------------------------------------------------------------
    // "Previous" requests (navigating backward)
    // -----------------------------------------------------------------------

    #[test]
    fn test_previous_request_no_more() {
        let list_params = ListParamsBuilder::default()
            .len(5usize)
            .previous("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(5);
        let result = make_items(3);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_previous_request_exact_len_no_more_back() {
        // N+1 fix: exactly len rows on a "previous" request → no more backward
        let list_params = ListParamsBuilder::default()
            .len(3usize)
            .previous("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(3);
        let result = make_items(3);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_previous_request_has_more() {
        let list_params = ListParamsBuilder::default()
            .len(3usize)
            .previous("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(3);
        let result = make_items(4);
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_previous_request_reverses_result() {
        let list_params = ListParamsBuilder::default()
            .len(5usize)
            .previous("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(5);
        let result = vec![make_dto("c"), make_dto("b"), make_dto("a")];
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(*resp.data[0].a, "a");
        assert_eq!(*resp.data[1].a, "b");
        assert_eq!(*resp.data[2].a, "c");
    }

    #[test]
    fn test_duplicate_cursor_value_is_an_error() {
        let list_params = ListParamsBuilder::default().len(10usize).build().unwrap();
        let query_params = make_query_params(10);
        let result = vec![make_dto("a"), make_dto("b"), make_dto("a")];
        let err = build_list_response(&list_params, &query_params, result).unwrap_err();
        let err = err.domain_err::<crate::list::params::ListError>();
        assert!(matches!(
            err,
            crate::list::params::ListError::NonUniquePaginationKey(_, _)
        ));
    }

    // -----------------------------------------------------------------------
    // Cursor values
    // -----------------------------------------------------------------------

    #[test]
    fn test_next_cursor_points_to_last_item() {
        let list_params = ListParamsBuilder::default().len(2usize).build().unwrap();
        let query_params = make_query_params(2);
        let result = vec![make_dto("first"), make_dto("second"), make_dto("extra")];
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.next_pagination_id.as_deref(), Some("second"));
    }

    #[test]
    fn test_previous_cursor_points_to_first_item() {
        let list_params = ListParamsBuilder::default()
            .len(5usize)
            .next("cursor".to_string())
            .pagination_id("id".to_string())
            .build()
            .unwrap();
        let query_params = make_query_params(5);
        let result = vec![make_dto("first"), make_dto("second")];
        let resp = build_list_response(&list_params, &query_params, result).unwrap();
        assert_eq!(resp.previous_pagination_id.as_deref(), Some("first"));
    }
}
