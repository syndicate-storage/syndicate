#!/bin/bash
ADMIN_EMAIL=""
MS_HOST="localhost"
if [ "$#" -ge 1 ]; then
    ADMIN_EMAIL=$1
fi

if [ "$#" -ge 2 ]; then
    MS_HOST=$2
fi

# copy from template
cp Dockerfile.template Dockerfile
PARAM='s/\$MS_APP_ADMIN_EMAIL\$/'$ADMIN_EMAIL'/g'
sed -i $PARAM Dockerfile
PARAM='s/\$MS_HOST\$/'$MS_HOST'/g'
sed -i $PARAM Dockerfile

#docker build --no-cache -t docker-ms .
docker build -t syndicate-ms .
