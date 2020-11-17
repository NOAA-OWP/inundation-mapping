#!/bin/bash

umask 002
echo "Group ID: $GID | Group Name: $GN"
newgrp $GN
exec "$@"
