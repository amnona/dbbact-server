#!/bin/bash

# delete the test database and user and create new
/Applications/Postgres.app/Contents/MacOS/bin/psql postgres < create_test_db.commands.txt

# get the full database scheme
/Applications/Postgres.app/Contents/MacOS/bin/pg_restore -U dbbact_test -d dbbact_test --schema-only --no-owner ../database/dbbact-full-2019-04-02.psql

# add anonymous user
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO UsersTable (id,username) VALUES(0,'na');"
 # password hash is for empty string ""
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO UsersPrivateTable (id,username,passwordhash, attemptscounter) VALUES(0,'na','"'$2a$06$KNMw2Tjs8MF2RKL2O9UeGuyy9/lJBaouVN5byaLo/Sm33OMf7Uk3K'"',0);"

 # and run the test
 ./test_server.py
 