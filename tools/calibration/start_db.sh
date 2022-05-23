#!/bin/bash

# CALIBRATION_DB_USER_NAME and CALIBRATION_DB_PASS must be defined in params.env
docker-compose --env-file ../../config/params.env up --build -d
