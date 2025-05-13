use diesel::backend::Backend;
use diesel::connection::LoadConnection;
use diesel::deserialize::FromSql;
use diesel::expression::QueryMetadata;
use diesel::prelude::*;
use diesel::query_builder::{AsQuery, Query, QueryFragment, QueryId};
use diesel::row::{Field, Row};
use diesel::sql_types::{BigInt, Bool, Nullable, SingleValue, SmallInt};
use diesel::sql_types::{Integer, Text};
use facet::{Facet, HeapValue, NumberBits, ScalarAffinity, Shape, Wip};

trait FacetHelper<DB: Backend + 'static> {
    const DESERIALIZER: &[DeserializerFunction<DB>];
    const TARGET_TYPES: &[&Shape];
}

type DeserializerFunction<DB> = for<'a> fn(
    facet::Wip<'a>,
    value: Option<<DB as Backend>::RawValue<'_>>,
) -> QueryResult<facet::Wip<'a>>;

macro_rules! single_impl {
    ($rust_type: ty, $sql_type: ty) => {
        impl<DB> FacetHelper<DB> for $sql_type
        where
            DB: Backend + 'static,
            $rust_type: FromSql<$sql_type, DB>,
        {
            const DESERIALIZER: &[DeserializerFunction<DB>] = &[|wip, value| {
                wip.put(
                    <$rust_type as FromSql<Self, DB>>::from_nullable_sql(value)
                        .map_err(diesel::result::Error::DeserializationError)?,
                )
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
            }];

            const TARGET_TYPES: &[&Shape] = &[<$rust_type as Facet>::SHAPE];
        }

        impl<DB> FacetHelper<DB> for Nullable<$sql_type>
        where
            DB: Backend + 'static,
            Option<$rust_type>: FromSql<Self, DB>,
        {
            const DESERIALIZER: &[DeserializerFunction<DB>] = &[|wip, value| {
                wip.put(
                    <Option<$rust_type> as FromSql<Self, DB>>::from_nullable_sql(value)
                        .map_err(diesel::result::Error::DeserializationError)?,
                )
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
            }];

            const TARGET_TYPES: &[&Shape] = &[<Option<$rust_type> as Facet>::SHAPE];
        }
    };
}

single_impl!(i32, Integer);
single_impl!(i64, BigInt);
single_impl!(String, Text);
single_impl!(i16, SmallInt);
single_impl!(bool, Bool);

macro_rules! tuple_impls {
    ($(
        $Tuple:tt {
            $(($idx:tt) -> $T:ident, $ST:ident, $TT:ident,)+
        }
    )+) => {
        $(
            impl<__DB, $($T,)+> FacetHelper<__DB> for ($($T,)*)
            where
                __DB: Backend + 'static,
                $($T: FacetHelper<__DB> + SingleValue,)*
            {
                const DESERIALIZER: &[DeserializerFunction<__DB>] =
                    &[$($T::DESERIALIZER[0],)*];

                const TARGET_TYPES: &[&Shape] = &[$($T::TARGET_TYPES[0],)*];
            }
        )*
    };
}

diesel_derives::__diesel_for_each_tuple!(tuple_impls);

pub trait FacetRunQuery<C> {
    fn load_by_order<'f, F>(self, conn: &mut C) -> QueryResult<Vec<F>>
    where
        F: Facet<'f>;

    fn load_by_name<'f, F>(self, conn: &mut C) -> QueryResult<Vec<F>>
    where
        F: Facet<'f>;
}

impl<Q, C> FacetRunQuery<C> for Q
where
    C: LoadConnection + 'static,
    Q: AsQuery,
    Q::Query: Query<SqlType = Q::SqlType> + QueryFragment<C::Backend> + QueryId,
    C::Backend: QueryMetadata<Q::SqlType>,
    Q::SqlType: FacetHelper<C::Backend>,
{
    fn load_by_order<'f, F>(self, conn: &mut C) -> QueryResult<Vec<F>>
    where
        F: Facet<'f>,
    {
        query(self.as_query(), conn)
    }

    fn load_by_name<'f, F>(self, conn: &mut C) -> QueryResult<Vec<F>>
    where
        F: Facet<'f>,
    {
        query_named(self.as_query(), conn)
    }
}

fn query<'f, Q, T, C>(query: Q, conn: &mut C) -> QueryResult<Vec<T>>
where
    T: Facet<'f>,
    C: LoadConnection + 'static,
    Q: Query + QueryFragment<C::Backend> + QueryId,
    C::Backend: QueryMetadata<Q::SqlType>,
    Q::SqlType: FacetHelper<C::Backend>,
{
    const {
        let shape = T::SHAPE;
        match shape.ty {
            facet::Type::User(facet::UserType::Struct(s)) => {
                if s.fields.len() != Q::SqlType::DESERIALIZER.len() {
                    panic!("Expect the same number of fields in your rust struct as in your query");
                }
                let mut counter = 0;
                while counter < s.fields.len() {
                    let diesel_target_type = Q::SqlType::TARGET_TYPES[counter];
                    let facet_type = s.fields[counter].shape();
                    counter += 1;
                    if !equal_types(diesel_target_type, facet_type) {
                        panic!("Mismatching types expected");
                    }
                }
            }
            _ => panic!("This methods only accepts structs"),
        }
    }
    let row_iter = conn.load(query)?;
    let mut out = Vec::new();
    for row in row_iter {
        let row = row?;
        let wip = Wip::alloc::<T>()
            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;

        out.push(
            to_facet(wip, row, Q::SqlType::DESERIALIZER)?
                .materialize()
                .expect("We checked that we should initialize all fields in the const block"),
        );
    }

    Ok(out)
}

const fn equal_types(a: &Shape, b: &Shape) -> bool {
    let defs = (a.def, b.def);
    match_defs(defs)
}

const fn match_defs(defs: (facet::Def, facet::Def)) -> bool {
    match defs {
        (facet::Def::Scalar(s1), facet::Def::Scalar(s2)) => match (s1.affinity, s2.affinity) {
            (ScalarAffinity::String(s1), ScalarAffinity::String(s2)) => {
                match (s1.max_inline_length, s2.max_inline_length) {
                    (Some(s1), Some(s2)) => s1 == s2,
                    (None, None) => true,
                    _ => false,
                }
            }
            (ScalarAffinity::Boolean(_b1), ScalarAffinity::Boolean(_b2)) => true,
            (ScalarAffinity::UUID(_u1), ScalarAffinity::UUID(_u2)) => true,
            (ScalarAffinity::IpAddr(_ip1), ScalarAffinity::IpAddr(_ip2)) => true,
            (ScalarAffinity::Number(n1), ScalarAffinity::Number(n2)) => match (n1.bits, n2.bits) {
                (
                    NumberBits::Integer {
                        bits: bits1,
                        sign: sign1,
                    },
                    NumberBits::Integer {
                        bits: bits2,
                        sign: sign2,
                    },
                ) => {
                    bits1 == bits2
                        && match (sign1, sign2) {
                            (facet::Signedness::Signed, facet::Signedness::Signed) => true,
                            (facet::Signedness::Signed, facet::Signedness::Unsigned) => false,
                            (facet::Signedness::Unsigned, facet::Signedness::Signed) => false,
                            (facet::Signedness::Unsigned, facet::Signedness::Unsigned) => true,
                        }
                }
                (
                    NumberBits::Float {
                        sign_bits: sign_bits1,
                        exponent_bits: exponent_bits1,
                        mantissa_bits: mantissa_bits1,
                        has_explicit_first_mantissa_bit: has_explicit_first_mantissa_bit1,
                    },
                    NumberBits::Float {
                        sign_bits: sign_bits2,
                        exponent_bits: exponent_bits2,
                        mantissa_bits: mantissa_bits2,
                        has_explicit_first_mantissa_bit: has_explicit_first_mantissa_bit2,
                    },
                ) => {
                    sign_bits1 == sign_bits2
                        && exponent_bits1 == exponent_bits2
                        && mantissa_bits1 == mantissa_bits2
                        && has_explicit_first_mantissa_bit1 == has_explicit_first_mantissa_bit2
                }
                _ => false,
            },
            _ => false,
        },
        (facet::Def::Option(s1), facet::Def::Option(s2)) => match_defs((s1.t.def, s2.t.def)),
        _ => false,
    }
}

fn query_named<'f, Q, T, C>(query: Q, conn: &mut C) -> QueryResult<Vec<T>>
where
    T: Facet<'f>,
    C: LoadConnection + 'static,
    Q: Query + QueryFragment<C::Backend> + QueryId,
    C::Backend: QueryMetadata<Q::SqlType>,
    Q::SqlType: FacetHelper<C::Backend>,
{
    const {
        if !matches!(T::SHAPE.ty, facet::Type::User(facet::UserType::Struct(_))) {
            panic!("Only structs expetced")
        }
    }
    // todo: we need some const checks here
    let row_iter = conn.load(query)?;
    let mut out = Vec::new();
    let facet::Type::User(facet::UserType::Struct(s)) = T::SHAPE.ty else {
        // checked in the const block above
        unreachable!()
    };
    for row in row_iter {
        let row = row?;
        let wip = Wip::alloc::<T>()
            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;

        out.push(
            to_named_facet(wip, row, Q::SqlType::DESERIALIZER, s.fields)?
                .materialize()
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?,
        );
    }

    Ok(out)
}

fn to_named_facet<'a, 'b, DB: Backend>(
    mut wip: Wip<'a>,
    row: impl Row<'b, DB>,
    deserializers: &[DeserializerFunction<DB>],
    fields: &[facet::Field],
) -> QueryResult<HeapValue<'a>>
where
    DB: Backend + 'static,
{
    for field in fields {
        let idx = row.idx(field.name).ok_or_else(|| {
            diesel::result::Error::DeserializationError(Box::new(
                diesel::result::UnexpectedEndOfRow,
            ))
        })?;
        let d = &deserializers[idx];
        wip = wip
            .field_named(field.name)
            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
        wip = read_field_to_facet(wip, &row, idx, d)?;
    }
    wip.build()
        .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
}

// row and DB are once by database backend
// so you should only have a low number of instances of that
// function in the final binary
fn to_facet<'a, 'b, DB: Backend>(
    mut wip: Wip<'a>,
    row: impl Row<'b, DB>,
    deserializers: &[DeserializerFunction<DB>],
) -> QueryResult<HeapValue<'a>>
where
    DB: Backend + 'static,
{
    for (f, d) in (0..row.field_count()).zip(deserializers) {
        wip = wip
            .field(f)
            .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
        wip = read_field_to_facet(wip, &row, f, d)?;
    }
    wip.build()
        .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
}

fn read_field_to_facet<'a, 'b, DB>(
    mut wip: Wip<'a>,
    row: &impl Row<'b, DB>,
    f: usize,
    d: &DeserializerFunction<DB>,
) -> QueryResult<Wip<'a>>
where
    DB: Backend + 'static,
{
    let field = row
        .get(f)
        .ok_or_else(|| diesel::result::UnexpectedEndOfRow)
        .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
    wip = d(wip, field.value())?;
    wip = wip
        .pop()
        .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
    Ok(wip)
}

#[test]
fn test() {
    use diesel::connection::SimpleConnection;

    let mut conn = SqliteConnection::establish(":memory:").unwrap();

    conn.batch_execute(
        "
CREATE TABLE users(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, hair_color TEXT);
INSERT INTO users(name, hair_color) VALUES ('John', 'blue'), ('Jane', NULL);
",
    )
    .unwrap();

    #[derive(Facet, Debug, PartialEq)]
    struct User {
        id: i32,
        name: String,
        hair_color: Option<String>,
    }

    #[derive(Facet, Debug, PartialEq)]
    struct UserOtherWayAround {
        name: String,
        hair_color: Option<String>,
        id: i32,
    }

    table! {
        users {
            id -> Integer,
            name -> Text,
            hair_color -> Nullable<Text>,
        }
    }

    let res1 = users::table.load_by_order::<User>(&mut conn);
    assert_eq!(
        res1,
        Ok(vec![
            User {
                id: 1,
                name: "John".into(),
                hair_color: Some("blue".into())
            },
            User {
                id: 2,
                name: "Jane".into(),
                hair_color: None,
            }
        ])
    );
    let res2 = users::table.load_by_name::<UserOtherWayAround>(&mut conn);
    assert_eq!(
        res2,
        Ok(vec![
            UserOtherWayAround {
                id: 1,
                name: "John".into(),
                hair_color: Some("blue".into()),
            },
            UserOtherWayAround {
                id: 2,
                name: "Jane".into(),
                hair_color: None,
            }
        ])
    );

    let res3 = users::table
        .filter(users::id.eq(1))
        .select((users::id, users::name, users::hair_color))
        .load_by_order::<User>(&mut conn);
    assert_eq!(
        res3,
        Ok(vec![User {
            id: 1,
            name: "John".into(),
            hair_color: Some("blue".into())
        },])
    );
}
