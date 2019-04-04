#!/bin/bash

# delete the test database and user and create new
/Applications/Postgres.app/Contents/MacOS/bin/psql postgres < create_test_db.commands.txt

# get the full database scheme
/Applications/Postgres.app/Contents/MacOS/bin/pg_restore -U dbbact_test -d dbbact_test --schema-only --no-owner ../database/dbbact-full-2019-04-02.psql

# add anonymous user
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO UsersTable (id,username) VALUES(0,'na');"
 # password hash is for empty string ""
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO UsersPrivateTable (id,username,passwordhash, attemptscounter) VALUES(0,'na','"'$2a$06$KNMw2Tjs8MF2RKL2O9UeGuyy9/lJBaouVN5byaLo/Sm33OMf7Uk3K'"',0);"

 # add annotationtypes
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationTypesTable (id,description) VALUES(1,'diffexp');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationTypesTable (id,description) VALUES(2,'isa');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationTypesTable (id,description) VALUES(3,'contamination');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationTypesTable (id,description) VALUES(4,'common');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationTypesTable (id,description) VALUES(5,'highfreq');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationTypesTable (id,description) VALUES(6,'other');"

# add details types
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationDetailsTypesTable (id,description) VALUES(1,'high');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationDetailsTypesTable (id,description) VALUES(2,'low');"
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AnnotationDetailsTypesTable (id,description) VALUES(3,'all');"

# add agents type
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO AgentTypesTable (id,description) VALUES(0,'na');"
# add methods type
 /Applications/Postgres.app/Contents/MacOS/bin/psql -d dbbact_test -U dbbact_test -c "INSERT INTO MethodTypesTable (id,description) VALUES(0,'na');"

 # and run the test
 ./test_server.py
 