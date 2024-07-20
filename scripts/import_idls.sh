#! /bin/bash

PATH_TO_SB_DL="target/release/sb_dl"
PATH_TO_IDLS="$1"

find "$PATH_TO_IDLS" -type f -name "*.json" | while read -r IDL; do
    PROGRAM_ID=$(echo "$IDL" | awk -F '/' '{print $NF}' | awk -F '_' '{print $1}')
    "$PATH_TO_SB_DL" manual-idl-import --input "$IDL" --program-id "$PROGRAM_ID"
done