#!/bin/bash
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 admin_email"
    exit 1
fi

# copy from template
cp Dockerfile.template Dockerfile
PARAM='s/\$MS_APP_ADMIN_EMAIL\$/'$1'/g'
sed -i $PARAM Dockerfile

#docker build --no-cache -t docker-ms .
docker build -t syndicate-ms .
