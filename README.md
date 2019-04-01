# dbBact

This is the REST-API server for the dbBact open microbiome knowledge-base.

The main dbBact server is running at: [dbbact.org](dbbact.org).

The dbBact API is available at: api.dbbact.org

API documentation is available at: [api.dbbact.org/docs](api.dbbact.org/docs)

dbBact is used by the [Calour microbiome analysis tool](https://github.com/biocore/calour)

## Installation:
<Warning>
We strongly recommend using the main running dbBact server (api.dbbact.org).

By locally installing dbBact, the local installation will not be synchronized with the main dbBact server.
</Warning>

However, you may want to locally install dbBact in order to test new functions, or to run local queries.

1. dbBact server interfaces with a postgres SQL database containing the actual data. dbBact requires postgres 9.3 or higher. postgres can be obtained [here](https://www.postgresql.org/).

2. Create the dbBact conda environment:
conda create -n dbbact python=3.5 numpy matplotlib scipy jupyter statsmodels
source acticate dbbact

3. install more required python packages:
pip install flask-cors
pip install image
pip install git+git://github.com/amueller/word_cloud
pip install psycopg2

3. Clone the dbbact github repository
git clone 
