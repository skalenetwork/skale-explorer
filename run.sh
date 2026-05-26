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

if [ "$OPTION" = "--update" ]; then
  BLOCKSCOUT_TAG=$(grep -E '^BLOCKSCOUT_BACKEND_DOCKER_TAG=' "$WORKDIR/.env" | cut -d '=' -f2-)
  if [ -n "$BLOCKSCOUT_TAG" ]; then
    echo "Checking out deps/blockscout to tag $BLOCKSCOUT_TAG..."
    git -C "$WORKDIR/deps/blockscout" fetch --tags
    git -C "$WORKDIR/deps/blockscout" checkout "$BLOCKSCOUT_TAG"
  fi
fi

HOST_DIR_PATH=$WORKDIR OPTION=$OPTION docker compose -f $WORKDIR/docker-compose.yaml up -d --build
