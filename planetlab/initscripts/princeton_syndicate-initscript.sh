#!/bin/sh

SERVER=http://vcoblitz-cmi.cs.princeton.edu/
CRON=/caches/squid-planetlab.cron
CRONTAB=/caches/crontab

sudo yum -y update
sudo yum -y install subversion gcc gcc-c++ gnutls-devel openssl-devel boost-devel libxml2-devel libattr-devel curl-devel automake make libtool fcgi-devel texinfo fuse fuse-devel libgcrypt-devel python-uuid uriparser-devel wget openssh-clients protobuf protobuf-devel protobuf-compiler libssh2-devel epel-release squid curl cronie-noanacron lsof

curl $SERVER/$CRON > /tmp/squid.cron && chmod +x /tmp/squid.cron && sudo mv /tmp/squid.cron /etc/cron.hourly/
curl $SERVER/$CRONTAB > /tmp/crontab && sudo mv /tmp/crontab /etc/crontab

sudo /etc/init.d/crond restart

sudo /etc/cron.hourly/squid.cron
