#!/bin/bash

read -sp "Enter password: " passw && echo \n && [[ $passw == "deleteIt" ]] || exit 1;
source .env;

echo "Choose which to delete:
1. Extracted files ($DIRECTORY_TO_EXTRACT_TO)
2. SQLite3 database file ($SQLITE3_DB_FILE)
3. Progress tracker ($PROGRESS_LOG_FILE)";

read selection;
selection="${selection:-123}";
read -p "Continue? (Y/N): " confirm && echo \n && [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]] || exit 1;

if [[ "$selection" == *"1"* ]]; then
    rm -r "$DIRECTORY_TO_EXTRACT_TO"*;
fi;
if [[ "$selection" == *"2"* ]]; then
    rm "$SQLITE3_DB_FILE";
fi;
if [[ "$selection" == *"3"* ]]; then
    rm "$PROGRESS_LOG_FILE";
fi;