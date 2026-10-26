//
// Copyright 2026 Tabsdata Inc.
//

use crate::dxo::crudl::ListResponse;
use crate::list::params::{ListError, ListQueryParams};
use crate::list::sort::Toward;
use std::any::type_name;
use std::collections::HashSet;
use ta_objects::ListDto;
use td_error::TdError;

impl<D: ListDto> ListQueryParams<D> {
    /// The page of `rows` the listing read: one past `len` says there is more.
    pub fn respond(self, mut rows: Vec<D>) -> Result<ListResponse<D>, TdError> {
        // A repeated cursor value means the pagination column is not unique here:
        // the next page would silently skip the rest of the tie group.
        let mut seen = HashSet::with_capacity(rows.len());
        for row in &rows {
            if !seen.insert(row.pagination_value()) {
                return Err(ListError::NonUniquePaginationKey(
                    type_name::<D>().to_string(),
                    row.pagination_value(),
                )
                .into());
            }
        }

        let len = *self.len as usize;
        let has_more = rows.len() > len;
        rows.truncate(len);

        let toward = self.sort.toward();
        if toward == Some(Toward::Previous) {
            rows.reverse();
        }

        let first = match (toward, rows.first()) {
            // A next page always has one before it; a previous page only when
            // the read found more rows further back.
            (Some(Toward::Next), first) => first,
            (Some(Toward::Previous), first) if has_more => first,
            _ => None,
        };
        let last = match (toward, rows.last()) {
            (Some(Toward::Previous), last) => last,
            (_, last) if has_more => last,
            _ => None,
        };
        let primary = self.sort.primary();
        let cursor = |row: Option<&D>| match row {
            Some(row) => (row.order_by_value(&primary), Some(row.pagination_value())),
            None => (None, None),
        };
        let (previous, previous_pagination_id) = cursor(first);
        let (next, next_pagination_id) = cursor(last);

        Ok(ListResponse {
            list_params: self.request,
            len: rows.len(),
            data: rows,
            previous,
            previous_pagination_id,
            next,
            next_pagination_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::crudl::ListParams;
    use crate::types::basic::PageLen;
    use ta_objects::FromDao;
    use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    pub struct StringType(String);

    #[ta_objects::Dao]
    #[dao(table = "testdao", ops(select, insert, delete))]
    struct TestDao {
        #[dao(pk)]
        pub a: StringType,
    }

    #[ta_objects::Dto]
    #[dto(list(on = TestDao))]
    struct TestDto {
        #[dto(list(pagination_by = "+"))]
        a: StringType,
    }

    impl FromDao for TestDto {
        type Dao = TestDao;

        fn from_dao(dao: TestDao) -> Result<Self, TdError> {
            Ok(Self { a: dao.a })
        }
    }

    fn make_dto(val: &str) -> TestDto {
        let dao = TestDao {
            a: val.try_into().unwrap(),
        };
        TestDto::from_dao(dao).unwrap()
    }

    fn make_items(n: usize) -> Vec<TestDto> {
        (0..n).map(|i| make_dto(&format!("item_{i}"))).collect()
    }

    enum Request {
        Initial,
        Next,
        Previous,
    }

    fn respond(
        len: i32,
        request: Request,
        rows: Vec<TestDto>,
    ) -> Result<ListResponse<TestDto>, TdError> {
        let cursor = || Some("cursor".to_string());
        let (previous, next) = match request {
            Request::Initial => (None, None),
            Request::Next => (None, cursor()),
            Request::Previous => (cursor(), None),
        };
        let pagination_id = previous
            .as_ref()
            .or(next.as_ref())
            .map(|_| "id".to_string());
        let params = ListParams {
            len: PageLen::try_from(len)?,
            previous,
            next,
            pagination_id,
            ..ListParams::default()
        };
        ListQueryParams::<TestDto>::try_from(params)?.respond(rows)
    }

    #[test]
    fn test_initial_empty_result() {
        let resp = respond(10, Request::Initial, vec![]).unwrap();
        assert_eq!(resp.data.len(), 0);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_initial_fewer_than_len() {
        let resp = respond(10, Request::Initial, make_items(5)).unwrap();
        assert_eq!(resp.data.len(), 5);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_initial_exact_len_no_next() {
        // Exactly `len` rows: the read found no extra one, so no next page.
        let resp = respond(3, Request::Initial, make_items(3)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_initial_has_more_truncates() {
        let resp = respond(3, Request::Initial, make_items(4)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.next_pagination_id.is_some());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_next_request_no_more() {
        let resp = respond(5, Request::Next, make_items(3)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_next_request_exact_len_no_more() {
        let resp = respond(3, Request::Next, make_items(3)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_none());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_next_request_has_more() {
        let resp = respond(3, Request::Next, make_items(4)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_previous_request_no_more() {
        let resp = respond(5, Request::Previous, make_items(3)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_previous_request_exact_len_no_more_back() {
        let resp = respond(3, Request::Previous, make_items(3)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_none());
    }

    #[test]
    fn test_previous_request_has_more() {
        let resp = respond(3, Request::Previous, make_items(4)).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert!(resp.next.is_some());
        assert!(resp.previous.is_some());
    }

    #[test]
    fn test_previous_request_reverses_result() {
        let rows = vec![make_dto("c"), make_dto("b"), make_dto("a")];
        let resp = respond(5, Request::Previous, rows).unwrap();
        assert_eq!(*resp.data[0].a, "a");
        assert_eq!(*resp.data[1].a, "b");
        assert_eq!(*resp.data[2].a, "c");
    }

    #[test]
    fn test_duplicate_cursor_value_is_an_error() {
        let rows = vec![make_dto("a"), make_dto("b"), make_dto("a")];
        let err = respond(10, Request::Initial, rows).unwrap_err();
        assert!(matches!(
            err.domain_err::<ListError>(),
            ListError::NonUniquePaginationKey(_, _)
        ));
    }

    #[test]
    fn test_next_cursor_points_to_last_item() {
        let rows = vec![make_dto("first"), make_dto("second"), make_dto("extra")];
        let resp = respond(2, Request::Initial, rows).unwrap();
        assert_eq!(resp.next_pagination_id.as_deref(), Some("second"));
    }

    #[test]
    fn test_previous_cursor_points_to_first_item() {
        let rows = vec![make_dto("first"), make_dto("second")];
        let resp = respond(5, Request::Next, rows).unwrap();
        assert_eq!(resp.previous_pagination_id.as_deref(), Some("first"));
    }

    #[test]
    fn test_the_response_echoes_the_request() {
        let resp = respond(7, Request::Next, vec![]).unwrap();
        assert_eq!(*resp.list_params.len, 7);
        assert_eq!(resp.list_params.next.as_deref(), Some("cursor"));
    }
}
