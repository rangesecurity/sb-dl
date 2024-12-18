// @generated automatically by Diesel CLI.

diesel::table! {
    use diesel::sql_types::*;

    blocks (number) {
        number -> Int8,
        slot -> Int8,
        time -> Nullable<Timestamptz>,
        processed -> Bool,
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

    squads (account) {
        account -> Varchar,
        vaults -> Array<Nullable<Text>>,
        members -> Array<Nullable<Text>>,
        threshold -> Int8,
        program_version -> Int8,
        voting_members_count -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    blocks,
    idls,
    programs,
    squads,
);
