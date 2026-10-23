#!/bin/bash
set -e

# Enforce default permissions for all files/folders created from this point forward.
# umask 0002 means: 
#   - New Folders get 775 (rwxrwxr-x)
#   - New Files get 664 (rw-rw-r--)
umask 0002

# Dynamically ensure runtime MPI bypass blocks are always globally active
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1

# Hand over process execution to whatever command was passed to the container (e.g. bash, python, jupyter)
echo "Running foss_fim docker container"

# Sets up files to be in valid group.
newgrp $GN

exec "$@"
