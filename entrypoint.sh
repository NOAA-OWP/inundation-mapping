#!/bin/bash
set -e

# Enforce default permissions for all files/folders created from this point forward.
# umask 0002 means: 
#   - New Folders get 775 (rwxrwxr-x)
#   - New Files get 664 (rw-rw-r--)
umask 0002

# Simply "source" the file to execute the exports
. ./system_optimization.sh

# Hand over process execution to whatever command was passed to the container (e.g. bash, python, jupyter)
echo "Running foss_fim docker container"

# Sets up files to be in valid group.
newgrp $GN

exec "$@"
