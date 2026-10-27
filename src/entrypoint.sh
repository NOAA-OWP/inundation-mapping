#!/bin/bash
set -e

umask 002
#echo "Group ID: $GID | Group Name: $GN"
echo "foss_fim docker"
newgrp $GN
exec "$@"
