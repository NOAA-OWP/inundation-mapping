#!/bin/bash

# Why is this in the src dir and not root?

umask 002
echo "Group ID: $GID | Group Name: $GN"
newgrp $GN
exec "$@"
