#!/usr/bin/env bash

set -e
OPTION=""

# Check for command-line arguments
case "$1" in
  --verify)
    OPTION="--verify"
    ;;
  --update)
    OPTION="--update"
    ;;
  --down)
    OPTION="--down"
    ;;
  --restart)
    OPTION="--restart"
    ;;
  *)
    OPTION=""
    ;;
esac

WORKDIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
HOST_DIR_PATH=$WORKDIR OPTION=$OPTION docker compose -f $WORKDIR/docker-compose.yaml up -d --build
