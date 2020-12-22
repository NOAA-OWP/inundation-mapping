#!/bin/sh

cd /opt/connector/
echo "Starting Gunicorn"
exec gunicorn -k gevent -w 1 --bind 0.0.0.0:6000 --log-level=warning --reload wsgi:app
