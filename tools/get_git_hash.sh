#!/usr/bin/env bash
echo 'Saving '`pwd`'/config/params.env'

cp config/params_template.env config/params.env

echo -e '\n#### GitHub commit hash ####' >> config/params.env

echo 'export commit_hash='`(git rev-parse --short HEAD)` >> config/params.env
