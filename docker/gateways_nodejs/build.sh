#!/bin/bash
GATEWAY_PORT=31111
IMAGE_NAME="syndicate-gateways-nodejs"
if [ "$#" -ge 1 ]; then
    GATEWAY_PORT=$1
fi

if [ "$#" -ge 2 ]; then
    IMAGE_NAME=$2
fi

# copy from template
cp Dockerfile.template Dockerfile
PARAM='s/\$GATEWAY_PORT\$/'$GATEWAY_PORT'/g'
sed -i $PARAM Dockerfile

#docker build --no-cache -t $IMAGE_NAME .
docker build -t $IMAGE_NAME .
