#!/bin/sh

SERVER=http://vcoblitz-cmi.cs.princeton.edu/
CRON=/caches/squid-vicci.cron
CRONTAB=/caches/crontab

yum -y update
yum -y install subversion gcc gcc-c++ gnutls-devel openssl-devel boost-devel libxml2-devel libattr-devel curl-devel automake make libtool fcgi-devel texinfo fuse fuse-devel libgcrypt-devel python-uuid uriparser-devel wget openssh-clients protobuf protobuf-devel protobuf-compiler libssh2-devel epel-release squid curl cronie-noanacron lsof

curl $SERVER/bind_public/bind_public.c > /tmp/bind_public.c && gcc -g -shared -fPIC -ldl /tmp/bind_public.c -o /usr/local/lib/bind_public.so
curl $SERVER/bind_public/ld.so.preload > /tmp/ld.so.preload && mv /tmp/ld.so.preload /etc/ld.so.preload

curl $SERVER/$CRON > /tmp/squid.cron && chmod +x /tmp/squid.cron && mv /tmp/squid.cron /etc/cron.hourly/
curl $SERVER/$CRONTAB > /tmp/crontab && mv /tmp/crontab /etc/crontab

/etc/init.d/crond restart

sh /etc/cron.hourly/squid.cron

