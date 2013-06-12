#!/bin/sh

SERVER=vcoblitz-cmi.cs.princeton.edu

sudo killall -2 yum
sleep 1
sudo killall -9 yum

sudo yum -y install subversion gcc gcc-c++ gnutls-devel openssl-devel boost-devel libxml2-devel libattr-devel curl-devel automake make libtool fcgi-devel texinfo fuse fuse-devel pyOpenSSL libgcrypt-devel python-uuid uriparser-devel wget openssh-clients protobuf protobuf-devel protobuf-compiler libssh2-devel squid daemonize texinfo 

mkdir /tmp/libmicrohttpd || exit 0

curl http://$SERVER/libmicrohttpd.tar.bz2 > /tmp/libmicrohttpd.tar.bz2
tar xvf /tmp/libmicrohttpd.tar.bz2 -C /tmp/libmicrohttpd

echo "before"
ls /usr/lib/libmicrohttpd*

cd /tmp/libmicrohttpd/libmicrohttpd-syndicate && ./bootstrap && ./configure --prefix=/usr/ && make && sudo make uninstall && sudo make install

echo "after"
ls /usr/lib/libmicrohttpd*
