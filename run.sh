#!/usr/bin/env bash

set -e

: "${ENDPOINT?Need to set ENDPOINT}"
: "${SCHAIN_PROXY_DOMAIN?Need to set SCHAIN_PROXY_DOMAIN}"

export FLASK_APP_HOST=0.0.0.0
export FLASK_APP_PORT=5000

WORKDIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
HOST_DIR_PATH=$WORKDIR docker-compose -f $WORKDIR/docker-compose.yaml up -d --build