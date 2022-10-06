
# This tool is a service that creates a postgres database used to update
# hydrotables.  It is generally only applicable to HUCs with AHPS libraries or 
# good USGS guages.

# This is not a manditory tool for use with FIM and is designed to run independently.
# If it is not setup or being used, ensure to shut the system off in the 
# FIM/config/params_template.env (or equiv). Change the following src_adjust_spatial="True"

# If you are not using this system, change that value to "False"


# -------------------------------
# Creating the calibration database service

1. Copy repository to permanent location (or at least the "calibration-db" folder).

2. Copy `calb_db_keys_template.env`, moving it into a folder outside the source folder (calibration-db). Rename the file (your choice of name).  ie) `config/calb_db_keys.env`

3. Update the values in the new .env file. 
	Note: The `CALIBRATION_DB_HOST` is the server where the docker container is physically running (aka.. the calibraion database server if you will). Multiple computers can share one calibration database and server. Each "client" computer can have its own calib_db_keys.env file and/or can share the .env with other computers.  ie) dev-1 and prod-1 can share a .env file if they have a shared drive.
	
4. Now start the service(from the service source code directory)
	  - `docker-compose --env-file {path to .env file}/{your key file name} up --build -d` 
	     ie) docker-compose --env-file {root folder path to .env}/config/calib_db_keys.env up --build -d

	   - If you get an error with permissions denied, you might need to upgrade the permissions to your calibration-db folder. from your tools directory, run `chmod -R 777 calibration-db` (yes.. 777)

5. Update the FIM/config/params_template.env for the following: src_adjust_spatial="True"

# -------------------------------
# Destroying the database (from the service source code directory)

`docker-compose down`
`rm -rf pgdata`

# -------------------------------

# Debugging Postgres DB

Use the following command to connect to the postgres DB:
`docker run -it --rm postgis/postgis psql -h SERVER_HOST_NAME -U DB_USERNAME` (and follow prompt to enter password)

## View names of databases (you can have more than one database in a postgres database server. ie) calibation and calibration-test.

`\l`

## Connect to calibration DB  

`\c calibration`  (or the name of the database you gave in the calib_db_keys.env)

## View tables in DB (now connected)

`\dt`

## View columns in tables

`\d+ hucs`
`\d+ points`

## View number of rows in a table

'select count(*) from table_name;  (you need the ";" on the end)


## Notes

The `docker-entrypoint-initdb.d` scripts only run once, when the DB is initially created. To rerun these scripts, you will need to destroy the database.
The `docker-entrypoint-initdb.d` scripts do not create the tables for the database, but rather simply enables the PostGIS extension.
If you are trying to connect to a database that is on the same server that you are working on, you have to use the full servername and not `localhost`.