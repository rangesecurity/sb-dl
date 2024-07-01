// @generated automatically by Diesel CLI.

diesel::table! {
    use diesel::sql_types::*;

    blocks (id) {
        id -> Uuid,
        number -> Int8,
        data -> Jsonb,
    }
}
