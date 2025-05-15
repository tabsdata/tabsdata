<!--
Copyright 2025 Tabs Data Inc.
-->

### TODO Move this to docs when documenting the API

# Pagination and Filtering API

## REST API

The REST API for pagination and filtering is as follows:

```http
GET /<PAGINATION_PATH>?<PAGINATION_PARAMS>
```

### Pagination REST API Parameters

* `order-by=<FIELD>+/-`: Order by given `<FIELD>`. `+/-` indicates ASC or DESC order, if not specified is ASC.
* `next`: Value of the `order-by` FIELD for the last record in the page, if `order-by` is not specified,
  the value of the `natural` order `<FIELD>` must be used. It cannot be used with `previous`. `id` must be specified.
* `previous`: Value of the `order-by` FIELD for the first record in the page, if `order-by` is not specified,
  the value of the `natural` order `<FIELD>` must be used. It cannot be used with `next`. `id` must be specified.
* `len`: Page length. If not specified, the default page length is used.
* `filter`: Filter conditions, there can be multiple `filter` parameters. See *List Filtering* section below for
  details.
* `id`: Value of the `<ID>` field of the record specified in `next` or `previous`.
  The API documentation will indicate which fields support `orderby`. Which ones are `orderby=id` and
  `orderby=natural`'. The default and maximum page length will also be indicated. And which fields
  support `filter` and `filter(like)`.

## Pagination Definitions

* The DTO struct must have an `id` column.
* The DTO must define what columns are sortable.
* The DTO struct must have a `natural` order column.
* List of DTOs are always sorted by a sortable column and their `id` column, in that order.
  If no specific order has been requested, the `natural` order is used.

* Doing and ASC sort means `column` ASC + `id` ASC.
* Doing and DESC sort means `column` DESC + `id` DESC.

## Specifying Sort Order

Specifying sort order: `&order-by=<COLNAME>+/-`.
The `+` means ASC, `-` means DESC. If no `+/-` is specified, the default is ASC.
*NOTE:* Only one sort column can be specified.

## Specifying a Page

It is possible to specify the next or previous page by using the `next` and `previous` parameters.
Next page: `&next=<COL_VALUE>&id=<ID>`
Previous page: `&previous=<COL_VALUE>&id=<ID>`
The column name is inferred from the `order-by` parameter. If not specified, the `natural` order column is used.

## Specifying Page Length

The length of the page can be specified using the `&len=<LEN>` parameter.

## Converting Pagination to SQL

### Next page ASC: `&order-by=COL+&next=VALUE&id=ID&len=LEN`

```sql
SELECT... FROM ... WHERE (...) AND $COL >= $VALUE AND id > $ID
ORDER BY $COL, id
LIMIT $LEN
```

### Previous page ASC: `&order-by=COL+&previous=VALUE&id=ID`

```sql  
SELECT... FROM ... WHERE (...) AND $COL <= $VALUE AND id < $ID
ORDER BY $COL DESC, id DESC
LIMIT $LEN
```

Then reverse the result.

### Next page DESC: `&order-by=COL-&next=VALUE&id=ID&len=LEN`

```sql
SELECT... FROM ... WHERE (...) AND $COL <= $VALUE AND id < $ID
ORDER BY $COL DESC, id DESC
LIMIT $LEN
```

### Previous page DESC: `&order-by=COL-&previous=VALUE&id=ID`

```sql
SELECT... FROM ... WHERE (...) AND $COL >= $VALUE AND id > $ID
ORDER BY $COL, id
LIMIT $LEN)
```

Then reverse the result.

## List Filtering

* Data has filter columns and filter-like columns.
* Data can only be filtered by filter and filter-like columns.
* filter columns support comparison operators.
* filter-like columns support comparison and like operators.
* filter-like columns must be string columns.
* multiple filters on the same column act as an OR.
* filters on different columns act as an AND.
* OR filters (same column) have precedence over AND filters (different columns).
  On Datetime columns:
* DTOS returned by the backend have datetime columns as unix epoch millis.
* Filters on datetime columns must be in unix epoch millis.
* The client (UI/CLI) must convert a date/datetime user input into unix epoch
  millis doing any TZ handling necessary including handling DATE only inputs
  (converting them to a DATETIME at the start of the day).

## Filtering REST API Parameters

Use the `filter` parameter to specify a filter. It can be specified multiple times to compose
filters.

```HTTP
GET /<PAGINATION_PATH>?filter=<COLNAME><OPERATOR><VALUE>&filter=<COLNAME><OPERATOR><VALUE>
```

Valid operators are:

* `:eq:` - equal.
* `:ne:` - not equal.
* `:gt:` - greater than.
* `:ge:` - greater or equal.
* `:lt:` - less than.
* `:le:` - less or equal.
* `:lk:` - like.
  For the `:lk:` operator, the wildcard character is `*`.

## Macros

## DTO macros

* Struct level macro: `#[td_type:page(default_len=#, max_len=#)]`
  (defaults: `default_len=100` and `max_len=1000`)
* Field annotations: `#[td_type:page(filter)]`, `#[td_type:page(filter=like)]` ,
  `#[td_type:page(orderby)]`, `#[td_type:page(orderby=natural)]`, `#[td_type:page(orderby=id)]`
  This info will be available in the generated OpenAPI documentation.
  This info must be used to implement the `ListQuery` trait impl for the `DTO` struct.
  The `ListQuery` trait is used for validation of the `ListParams` values.
  A `ListQuery` trait implementation must also be generated for the `$FROM` struct defined in the
  `DTO`'s `#[td_type::Dto] #[td_type(builder(try_from = $FROM))`. This trait implementation must take
  into account any field name mapping between the `DTO` and the `$FROM` structs.
  The `$FROM`'s `ListQuery` must have a `From` implementation for the `DTO`'s `ListQuery`.