
The calibration-db tool is an optional use tool. When started, it creates a service, via a docker container, that creates a postgres database. The calibratoin-db tool will make minor updates to hydrotables.csv's, but only to applicable HUCs. The applicable HUC's are generally have AHPS library or good USGS guages data.

To setup the system, it needs to be done in two parts.
A) Update the /config/params_template.env file and set the src_adjust_spatial to "True".  If you are not using the tool, set the flag to False.

B) Setup the calibration database service. See steps below.

# -------------------------------
# Creating the calibration database service

When you start up the service, it will start up a new docker container named "fim_calibration_db" and called by FIM code, if enabled in the params_template.env.

Steps:
1. Copy `/config/calb_db_keys_template.env` and rename it to a name of your choice (ie: calb_db_keys.env or whatever). We recommend saving the file outside the fim source code folder for security purposes. 

2. Update the values in the new .env file. 
	Note: The `CALIBRATION_DB_HOST` is the server where the docker container is physically running. This may be your computer/server name, but can be on a different server if you like. One calibration database service can be shared by multiple other servers, each with their own fim source code folders (a "client" if you will). Each "client" computer can have its own calib_db_keys.env file and/or can share the .env with other computers.  ie) dev-1 and prod-1 can share a .env file if they have a shared drive.
	
3. Now start the service from the `tools/calibration-db` directory of the server that will run the calibration database service. 
   Using bash, run
	  `docker-compose --env-file {path to .env file}/{your key file name} up --build -d` 
	  ie) docker-compose --env-file {root folder path to .env}/config/calib_db_keys.env up --build -d

(talk about command not found


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

![image](https://user-images.githubusercontent.com/90854818/194529110-866db185-c64d-4207-a3cd-d5ac12033dee.png)
