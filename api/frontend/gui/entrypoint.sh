#!/bin/sh

cd /opt/gui/
echo "Starting Gunicorn"
exec gunicorn --bind 0.0.0.0:5000 --reload wsgi:app