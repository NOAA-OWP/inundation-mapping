## The calibration database service

The **calibration-db tool** is an optional use tool. When started, it creates a service, via a docker container, that creates a postgres database. The calibration-db tool will make minor updates to hydrotables.csv's, but only to applicable HUCs. The applicable HUCs generally have AHPS library data or good USGS guage data.

-------------------------------
To setup the system, it needs to be done in two parts.

**A)** Update the `/config/params_template.env` file and set `src_adjust_spatial` to "True".  If you are not using the tool, set the flag to "False".

**B)** Setup the calibration database service. See steps below.

-------------------------------
## Creating the calibration database service

When you start the service, it will start up a new docker container named "fim_calibration_db", if enabled in the params_template.env.

To run commands, use a bash terminal window.

Steps:
1. Copy `/config/calb_db_keys_template.env` and rename it to a name of your choice (ie: `calb_db_keys.env` or whatever). We recommend saving the file outside the fim source code folder for security purposes. 

2. Update the values in the new .env file. 
	Note: The `CALIBRATION_DB_HOST` is the computer/server where the docker container is physically running. This may be your server name, but can also be a different server if you like. One calibration database service can be shared by multiple other servers, each with their own FIM source code folders (a "client" if you will). Each "client" server can have it's own calib_db_keys.env file and/or can share the .env with other servers.  ie) dev-1 and prod-1 can share a .env file if they have a shared drive.
	
3. Start the service from the /tools/calibration-db directory of the server that will run the calibration database service. 
   Using a bash terminal window, run
	  `docker-compose --env-file {path to .env file}/{your key file name} up --build -d` 
	  ie) docker-compose --env-file /my_server/config/calib_db_keys.env up --build -d

	   - If you get an error of permissions denied, you might need to upgrade the permissions to your calibration-db folder. From your /tools directory, run `chmod -R 777 calibration-db` (yes.. 777).  You may need to add the word "sudo" at the front of the command, depending on your system configuration.
	   - If you get an error saying `command not found: docker compose`, you may need to install it via `sudo apt install docker-compose` or check your system configuration.

4. You should be able to see a container named `fim_calibration_db` via a bash terminal command of `docker stats --no-stream`.

-------------------------------
## Destroying the database (from the /tools/calibration-db directory)

Using a bash terminal window, run
    `docker-compose down`
    `rm -rf pgdata`

-------------------------------
## Debugging Postgres DB

Use the following command to connect to the postgres DB (using values from your .env file):

`docker run -it --rm postgis/postgis psql -h CALIBRATION_DB_HOST -U CALIBRATION_DB_USER_NAME` (and follow prompt to enter password (CALIBRATION_DB_PASS))
ie) `docker run -it --rm postgis/postgis psql -h my_server_name -U fim_postgres` (and follow prompt to enter password (CALIBRATION_DB_PASS))

### View names of databases (you can have more than one database in a postgres database server. ie) calibation and calibration-test.

`\l`

    #### Note: FIM will create a database as it is being used. If you have not yet processed any HUCs, there will not be a database yet.

### Connect to calibration DB  

`\c calibration`  (or the name of the database you gave in the calib_db_keys.env (CALIBRATION_DB_NAME))

### View tables in DB (now connected)

`\dt`

### View columns in tables

`\d+ hucs`
`\d+ points`

### View number of rows in a table

'select count(*) from table_name;`  (you need the ";" on the end)

Postgres uses psql. See [https://www.postgresql.org/docs/current/sql-commands.html]9https://www.postgresql.org/docs/current/sql-commands.html) for details on commands.


## System Notes

The `docker-entrypoint-initdb.d` scripts only run once, when the DB is initially created. To rerun these scripts, you will need to destroy the database.
The `docker-entrypoint-initdb.d` scripts do not create the tables for the database, but rather simply enables the PostGIS extension.
If you are trying to connect to a database that is on the same server that you are working on, you have to use the full servername and not `localhost`.

