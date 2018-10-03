#!/bin/sh

# Copyright (c) 2018
# Cavium
#
# SPDX-License-Identifier: Apache-2.0
#

# Start EdgeX Foundry services in right order, as described:
# https://docs.edgexfoundry.org/Ch-GettingStartedUsers.html

if [[ -z $EDGEX_COMPOSE_FILE ]]; then
  COMPOSE_FILE=../docker/docker-compose.yml
  COMPOSE_URL=https://raw.githubusercontent.com/edgexfoundry/developer-scripts/master/compose-files/docker-compose-california-0.6.0.yml

  echo "Pulling latest compose file..."
  curl -o $COMPOSE_FILE $COMPOSE_URL
else
  COMPOSE_FILE=$EDGEX_COMPOSE_FILE
fi

EDGEX_CORE_DB=${EDGEX_CORE_DB:-"mongo"}

echo "Starting Mongo"
docker-compose -f $COMPOSE_FILE up -d mongo

if [[ ${EDGEX_CORE_DB} != mongo ]]; then
  echo "Starting $EDGEX_CORE_DB for Core Data Services"
  docker-compose -f $COMPOSE_FILE up -d $EDGEX_CORE_DB
fi

echo "Starting consul"
docker-compose -f $COMPOSE_FILE up -d consul
echo "Populating configuration"
docker-compose -f $COMPOSE_FILE up -d config-seed

echo "Sleeping before launching remaining services"
sleep 15

services="logging metadata data command export-client export-distro notifications"
for s in ${services}; do
    echo Starting ${s}
    docker-compose -f $COMPOSE_FILE up -d $s
done

