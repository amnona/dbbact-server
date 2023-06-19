For details about dbBact, see [Nucleic Acids Research 2023](https://doi.org/10.1093/nar/gkad527) paper.

# dbbact-server

This is the REST-API server for the dbBact open microbiome knowledge-base.

The main dbBact server is running at: [dbbact.org](http://dbbact.org).

The dbBact API is available at: api.dbbact.org

API documentation is available at: [api.dbbact.org/docs](http://api.dbbact.org/docs)

Example notebooks for using the dbBact REST-API are available at: [github.com/amnona/dbbact-examples](https://github.com/amnona/dbbact-examples)

dbBact is used by the [Calour microbiome analysis tool](https://github.com/biocore/calour)

## Installation:
<test class="warning">
We strongly recommend using the main running dbBact server (api.dbbact.org).

By locally installing dbBact, the local installation will not be synchronized with the main dbBact server.
</test>

However, you may want to locally install dbBact in order to test new functions, or to run local queries.

1. dbBact server interfaces with a postgres SQL database containing the actual data. dbBact requires postgres 9.3 or higher. postgres can be obtained [here](https://www.postgresql.org/).

2. Create the dbBact conda environment:
```
 conda create -n dbbact python=3.6 numpy matplotlib scipy jupyter statsmodels psycopg2 flask requests

 source activate dbbact
 ```

3. install additional needed packages:
```
pip install git+git://github.com/amnona/flask-autodoc

pip install flask-login
```

4. for gunicorn (alternatively can run using flask but not for production):
```
pip install gunicorn

pip install setproctitle
```

5. Clone the dbbact github repository
  git clone https://github.com/amnona/dbbact-server.git

6. change to the dbbact-server directory and install locally
```
cd dbbact-server

pip install -e .
```

7. install the database template to postgres
```
psql -U postgres < database/setup.psql

psql -U postgres -d dbbact < database/extensions.psql

pg_restore -U dbbact -d dbbact database/dbbact-export.psql
```
(Password for dbbact is magNiv)

Alternatively, to erase current dbbact database, use:
```
pg_restore -U postgres -d postgres --clean --create database/dbbact-export.psql
```
Also prepare the users private data table (which is not exported in the database export due to privacy concerns):
```
psql -U dbbact -d dbbact < database/data_users_private.txt
```

8. Set the dbbact server environment flag to specify running environment (production/develop/local)
production:
```
export DBBACT_SERVER_TYPE="production"
```
develop:
```
export DBBACT_SERVER_TYPE="develop"
```
local:
```
export DBBACT_SERVER_TYPE="local"
```
9. run locally:
NOTE: the default port for dbbact-server should be 5001
```
gunicorn 'dbbact_server.Server_Main:gunicorn(debug_level=5)' -b 127.0.0.1:5001 --workers 4 --name=dbbact-rest-api --timeout 300 --reload
```
