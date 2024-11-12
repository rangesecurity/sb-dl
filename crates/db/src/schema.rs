// @generated automatically by Diesel CLI.

diesel::table! {
    use diesel::sql_types::*;

    blocks (id) {
        id -> Uuid,
        number -> Int8,
        data -> Jsonb,
        slot -> Nullable<Int8>,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    blocks_2 (id) {
        id -> Uuid,
        number -> Int8,
        slot -> Nullable<Int8>,
        data -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    idls (id, begin_height) {
        id -> Varchar,
        begin_height -> Int8,
        end_height -> Nullable<Int8>,
        idl -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    programs (id, last_deployed_slot) {
        id -> Varchar,
        last_deployed_slot -> Int8,
        executable_account -> Varchar,
        executable_data -> Bytea,
    }
}

diesel::table! {
    use diesel::sql_types::*;

    squads (id) {
        id -> Uuid,
        account -> Varchar,
        vaults -> Array<Nullable<Text>>,
        members -> Array<Nullable<Text>>,
        treshold -> Int4,
        program_version -> Int4,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    blocks,
    blocks_2,
    idls,
    programs,
    squads,
);
