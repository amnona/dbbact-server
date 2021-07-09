#!/bin/bash
# run dbbact develop server on local mac
source activate dbbact2
echo "running dbbact API server local. to access send requests to http://127.0.0.1:7001"
export DBBACT_SERVER_TYPE="main"
gunicorn 'dbbact_server.Server_Main:gunicorn(debug_level=2)' -b 0.0.0.0:7001 --workers 1 --name=local-dbbact-rest-api --timeout 300 --reload
