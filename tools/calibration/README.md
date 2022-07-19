# Creating the database

1. Copy repository to permanent location
2. Duplicated `config/params_tempalte.env` as `config/params.env`
3. Define `CALIBRATION_DB_USER_NAME` and `CALIBRATION_DB_PASS` variables in `config/params.env`
4. Run `./start_db.sh`

# Debugging Postgres DB

Use the following command to connect to the postgres DB:
`docker run -it --rm postgis/postgis psql -h SERVER_HOST_NAME -U DB_USERNAME` (and follow prompt to enter password)

## Connect to calibration DB

`\c calibration`

## View tables in DB

`\dt`

## View columns in tables

`\d+ hucs`
`\d+ points`

# Destroying the database

`docker-compose down`
`rm -rf pgdata`

# Notes

The `docker-entrypoint-initdb.d` scripts only run once, when the DB is initially created. To rerun these scripts, you will need to destroy the database.
The `docker-entrypoint-initdb.d` scripts do not create the tables for the database, but rather simply enables the PostGIS extension.
If you are trying to connect to a database that is on the same server that you are working on, you have to use the full servername and not `localhost`.