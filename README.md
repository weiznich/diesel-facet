# Diesel-Facet

A integration for using [`#[derive(Facet)]`](https://docs.rs/crate/facet/latest/source/) to deserialize data coming from diesel queries.

```rust
use diesel::prelude::*;
use diesel_facet::FacetRunQuery;

#[derive(Facet)]
struct User {
    id: i32,
    name: String,
    hair_color: Option<String>,
}

table! {
    users {
        id -> Integer,
        name -> Text,
        hair_color -> Nullable<Text>,
    }
}

// construct diesel queryies as usual and
// use `load_by_order` to deserialize data
// via facet
users::table
    .filter(users::id.eq(1))
    .select((users::id, users::name, users::hair_color))
    .load_by_order::<User>(&mut conn)
```

## Limitations

* Only restricted set of types supported (i16, 132, 164, String, bool)
* No `#[diesel(embed)]` support
* Limited compile time checks for `FacetRunQuery::load_by_order` 
* `FacetRunQuery::load_by_name` doesn't provide any compile time checks yet

## Code of conduct

Anyone who interacts with Diesel in any space, including but not limited to
this GitHub repository, must follow our [code of conduct](https://github.com/diesel-rs/diesel/blob/master/code_of_conduct.md).

## License

Licensed under either of these:

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)
