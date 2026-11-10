//
// Copyright 2026 Tabsdata Inc.
//

//! A listing request parsed against its DTO: what to filter, how to sort and
//! where the page starts, as data. td-queries renders it.

use crate::dxo::crudl::ListParams;
use crate::list::columns::ColumnSelector;
use crate::list::filter::{Field, Filter};
use crate::list::sort::Sort;
use crate::list::tags::TagSelector;
use crate::types::basic::PageLen;
use ta_objects::{Dao, FromDao, ListDto, ListField, ListRole, SeaValue};
use td_error::{TdError, td_error};

#[td_error]
pub enum ListError {
    #[error("Invalid order-by value, it must be <NAME>+/-: {0}")]
    InvalidOrderBy(String) = 0,
    #[error("Invalid condition value, it must be <NAME><OPERATOR><VALUE> (operators are {0}): {1}")]
    InvalidCondition(String, String) = 1,
    #[error("Undefined filter field '{0}'; this list filters by: {1}")]
    UndefinedField(String, String) = 2,
    #[error("Undefined like filter field '{0}'; this list like-filters by: {1}")]
    UndefinedLikeFilter(String, String) = 4,
    #[error("Undefined order-by field '{0}'; this list orders by: {1}")]
    UndefinedOrderBy(String, String) = 5,
    #[error("Previous and Next parameters cannot be used together")]
    PreviousAndNext = 6,
    #[error("Previous and Next need a pagination id, and a pagination id needs one of them")]
    MissingPaginationParams = 7,
    #[error("Invalid between condition '{0}', it must be <NAME>:btw:<min>::<max>")]
    InvalidBetweenCondition(String) = 8,
    #[error("Undefined is filter: {0}")]
    UndefinedIsFilter(String) = 9,
    #[error("Error parsing value: {0}")]
    InvalidFieldValue(#[source] TdError) = 10,
    #[error("Empty value for filter '{0}'; use '{0}:is:null' to match NULLs")]
    EmptyFilterValue(String) = 11,
    #[error(
        "Pagination column of {0} is not unique within the listing (cursor value '{1}' repeats); paging would silently skip rows"
    )]
    NonUniquePaginationKey(String, String) = 5000,
}

/// The column identifiers of a list DTO's DAO.
pub type ListIden<D> = <<D as FromDao>::Dao as Dao>::Iden;

/// The fields a listing accepts in `role`, for an error.
pub(crate) fn accepted<D: ListDto>(role: ListRole) -> String {
    match D::fields(role)[..] {
        [] => "none".to_string(),
        ref fields => fields.join(", "),
    }
}

/// A request's value of `field`.
pub(crate) fn parse_value<D: FromDao>(
    field: &ListField<D>,
    value: &str,
) -> Result<Box<dyn SeaValue>, ListError> {
    (field.parse)(value).map_err(ListError::InvalidFieldValue)
}

/// Built only by parsing a request against `D`, as are its parts
/// (`#[non_exhaustive]`).
pub struct ListQueryParams<D: ListDto> {
    pub len: PageLen,
    pub columns: ColumnSelector<ListIden<D>>,
    pub tags: TagSelector<ListIden<D>>,
    pub sort: Sort<D>,
    /// Echoed by the response.
    pub(crate) request: ListParams,
}

impl<D: ListDto> TryFrom<ListParams> for ListQueryParams<D> {
    type Error = TdError;

    fn try_from(params: ListParams) -> Result<Self, Self::Error> {
        let mut columns = ColumnSelector::default();
        let mut tags = TagSelector::default();
        for filter in &params.filter {
            let parsed = Filter::parse(filter)?;
            match parsed.field {
                Field::Column(field) => columns.add::<D>(filter, field, parsed.op, parsed.value)?,
                Field::Tag { entity, key } => {
                    tags.add::<D>(filter, entity, key, parsed.op, parsed.value)?
                }
            }
        }
        let sort = Sort::parse(&params)?;
        Ok(Self {
            len: params.len,
            columns,
            tags,
            sort,
            request: params,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list::columns::{ColumnTest, Comparison};
    use crate::list::sort::{SortKeys, Toward};
    use ta_objects::SortDir;
    use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct StringType(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct RunName(String);

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
    struct RunEnded(String);

    #[ta_objects::Dao]
    #[dao(table = "rundao", ops(select, insert, delete))]
    struct RunDao {
        #[dao(pk)]
        id: StringType,
        name: RunName,
        ended: Option<RunEnded>,
    }

    #[ta_objects::Dto]
    #[dto(list(on = RunDao))]
    struct RunDto {
        #[dto(list(pagination_by = "+", filter))]
        id: StringType,
        #[dto(list(filter, filter_like, order_by))]
        name: RunName,
        #[dto(list(filter))]
        ended: Option<RunEnded>,
    }

    impl FromDao for RunDto {
        type Dao = RunDao;

        fn from_dao(dao: RunDao) -> Result<Self, TdError> {
            Ok(Self {
                id: dao.id,
                name: dao.name,
                ended: dao.ended,
            })
        }
    }

    type Iden = <RunDao as Dao>::Iden;

    fn parse(params: ListParams) -> Result<ListQueryParams<RunDto>, TdError> {
        ListQueryParams::try_from(params)
    }

    fn filtered(filters: &[&str]) -> Result<ListQueryParams<RunDto>, TdError> {
        parse(ListParams {
            filter: filters.iter().map(|f| String::from(*f)).collect(),
            ..ListParams::default()
        })
    }

    fn err(result: Result<ListQueryParams<RunDto>, TdError>) -> TdError {
        result.err().unwrap()
    }

    #[test]
    fn test_one_column_groups_its_tests() {
        let params = filtered(&["name:eq:a", "id:eq:x", "name:lk:b*"]).unwrap();
        let terms: Vec<_> = params
            .columns
            .0
            .iter()
            .map(|term| (term.column, term.tests.len()))
            .collect();
        assert_eq!(terms, [(Iden::Name, 2), (Iden::Id, 1)]);
        assert!(matches!(
            params.columns.0[0].tests[0],
            ColumnTest::Compare(Comparison::Eq, _)
        ));
        assert!(matches!(params.columns.0[0].tests[1], ColumnTest::Like(_)));
    }

    #[test]
    fn test_unknown_fields_name_what_the_list_accepts() {
        assert!(matches!(
            err(filtered(&["nope:eq:a"])).domain_err::<ListError>(),
            ListError::UndefinedField(field, accepted) if field == "nope" && accepted == "id, name, ended"
        ));
        assert!(matches!(
            err(filtered(&["id:lk:a"])).domain_err::<ListError>(),
            ListError::UndefinedLikeFilter(_, accepted) if accepted == "name"
        ));
        assert!(matches!(
            err(parse(ListParams {
                order_by: Some("nope".to_string()),
                ..ListParams::default()
            })).domain_err::<ListError>(),
            ListError::UndefinedOrderBy(_, accepted) if accepted == "id, name"
        ));
    }

    #[test]
    fn test_malformed_filters_are_rejected() {
        assert!(matches!(
            err(filtered(&["name:ff:a"])).domain_err::<ListError>(),
            ListError::InvalidCondition(..)
        ));
        assert!(matches!(
            err(filtered(&["ended:eq:"])).domain_err::<ListError>(),
            ListError::EmptyFilterValue(_)
        ));
        assert!(filtered(&["name:btw:a"]).is_err());
        assert!(filtered(&["name:btw:a::b::c"]).is_err());
        assert!(filtered(&["name:is:maybe"]).is_err());
        assert!(filtered(&["name:btw:a::b", "name:is:not_null", "name:lk:"]).is_ok());
    }

    /// The sort's columns and direction, and the cursor's direction.
    fn sorted(params: ListParams) -> (Option<Iden>, Iden, SortDir, Option<Toward>) {
        let sort = parse(params).unwrap().sort;
        let toward = sort.toward();
        match sort.keys {
            SortKeys::Tiebreak { tiebreak, .. } => (None, tiebreak, sort.dir, toward),
            SortKeys::By { by, tiebreak, .. } => (Some(by), tiebreak, sort.dir, toward),
        }
    }

    fn ordered(order_by: &str) -> ListParams {
        ListParams {
            order_by: Some(order_by.to_string()),
            ..ListParams::default()
        }
    }

    #[test]
    fn test_sort_defaults_to_the_pagination_column() {
        assert_eq!(
            sorted(ListParams::default()),
            (None, Iden::Id, SortDir::Asc, None)
        );
    }

    #[test]
    fn test_sort_by_a_column_tiebreaks_in_its_direction() {
        assert_eq!(
            sorted(ordered("name-")),
            (Some(Iden::Name), Iden::Id, SortDir::Desc, None)
        );
    }

    #[test]
    fn test_sort_keeps_the_field_it_orders_by_first() {
        let field = |params| parse(params).unwrap().sort.field.name;
        assert_eq!(field(ListParams::default()), "id");
        assert_eq!(field(ordered("name-")), "name");
        assert_eq!(field(ordered("id-")), "id");
    }

    #[test]
    fn test_sort_by_the_pagination_column_is_the_tiebreak() {
        assert_eq!(
            sorted(ordered("id-")),
            (None, Iden::Id, SortDir::Desc, None)
        );
        for bad in ["", "-", "name="] {
            assert!(
                matches!(
                    err(parse(ordered(bad))).domain_err::<ListError>(),
                    ListError::InvalidOrderBy(_)
                ),
                "{bad}"
            );
        }
    }

    fn paged(previous: Option<&str>, next: Option<&str>, id: Option<&str>) -> ListParams {
        ListParams {
            previous: previous.map(str::to_string),
            next: next.map(str::to_string),
            pagination_id: id.map(str::to_string),
            ..ordered("name")
        }
    }

    #[test]
    fn test_a_cursor_holds_a_value_per_sort_key() {
        let sort = parse(paged(None, Some("n"), Some("i"))).unwrap().sort;
        assert!(matches!(
            sort.keys,
            SortKeys::By {
                after: Some((Toward::Next, _, _)),
                ..
            }
        ));
        let sort = parse(paged(Some("n"), None, Some("i"))).unwrap().sort;
        assert_eq!(sort.toward(), Some(Toward::Previous));
        assert_eq!(sorted(paged(None, None, None)).3, None);
        let unsorted = ListParams {
            next: Some("i".to_string()),
            pagination_id: Some("i".to_string()),
            ..ListParams::default()
        };
        assert!(matches!(
            parse(unsorted).unwrap().sort.keys,
            SortKeys::Tiebreak {
                after: Some((Toward::Next, _)),
                ..
            }
        ));
    }

    #[test]
    fn test_cursor_needs_one_direction_and_an_id() {
        assert!(matches!(
            err(parse(paged(Some("p"), Some("n"), Some("i")))).domain_err::<ListError>(),
            ListError::PreviousAndNext
        ));
        for params in [
            paged(Some("p"), None, None),
            paged(None, Some("n"), None),
            paged(None, None, Some("i")),
        ] {
            assert!(matches!(
                err(parse(params)).domain_err::<ListError>(),
                ListError::MissingPaginationParams
            ));
        }
    }
}
