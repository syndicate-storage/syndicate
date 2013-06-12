#!/usr/bin/python

import os
import sys
import subprocess
import time
import socket
import urllib
import urllib2
import signal
import threading

SYNDICATE_ROOT = "/home/princeton_syndicate/syndicate"
YUM_PACKAGES = "subversion gcc gcc-c++ gnutls-devel openssl-devel boost-devel libxml2-devel libattr-devel curl-devel automake make libtool fcgi-devel texinfo fuse fuse-devel pyOpenSSL libgcrypt-devel python-uuid uriparser-devel wget openssh-clients protobuf protobuf-devel protobuf-compiler libssh2-devel squid daemonize"
DATA_SERVER_URL = "http://vcoblitz-cmi.cs.princeton.edu/"
BOOTSTRAP_NODE_FILE = "/tmp/bootstrap-node"
BOOTSTRAP_OUT = "/tmp/bootstrap.out"
TARGET="libmicrohttpd-install"

def sh(args):
   global BOOTSTRAP_OUT
   
   fd = open( BOOTSTRAP_OUT,"a")
   rc = subprocess.call(args, stdout=fd, stderr=fd)
   fd.close()
   return rc

   
# log a message
def log( loglevel, msg ):
   txt = "[%s] %s: %s" % (time.ctime(), loglevel, msg)
   print txt
   
# bootstrap the node
def bootstrap_node():
   global YUM_PACKAGES
   global BOOTSTRAP_NODE_FILE

   # don't bother if we already set everything up
   if os.path.exists( BOOTSTRAP_NODE_FILE ):
      try:
         bootstrap_fd = open( BOOTSTRAP_NODE_FILE, "r" )
         bootstrap_date = bootstrap_fd.read()
         bootstrap_fd.close()

         log( "INFO", "Node bootstrapped on %s" % bootstrap_date )
         return 0
      except Exception, e:
         log( "WARN", "Could not read %s (exception = %s), will re-bootstrap node" % (BOOTSTRAP_NODE_FILE, e ))
         os.unlink( BOOTSTRAP_NODE_FILE )

   # what FC version are we?
   fd = open("/etc/redhat-release", "r")
   rel = fd.read()
   fd.close()

   rel = rel.strip()
   log( "INFO", "redhat release: '%s'" % rel)

   # update
   rc = sh(["yum", "-y", "--nogpgcheck", "update"])
   if rc != 0:
      log( "ERROR", "bootstrap_node: 'yum update' rc = %s" % rc )
      return rc

   # install packages
   rc = sh(["yum", "-y", "--nogpgcheck", "install"] + YUM_PACKAGES.split())
   if rc != 0:
      log( "ERROR", "bootstrap_node: 'yum install' rc = %s" % rc )
      return rc

   log( "INFO", "successfully bootstrapped node" )

   date_ctime = time.ctime()
   bootstrap_fd = open( BOOTSTRAP_NODE_FILE, "w" )
   bootstrap_fd.write( date_ctime )
   bootstrap_fd.close()

   return 0


def make( target ):
   global SYNDICATE_ROOT
   rc = sh([ "make", "-C", SYNDICATE_ROOT, target])
   if rc != 0:
      log( "ERROR", "bootstrap_client: 'make %s' rc = %s" % (target, rc) )
   return rc


# bootstrap a Syndicate client
def bootstrap( target ):
   global SYNDICATE_ROOT
   global BOOTSTRAP_NODE_FILE

   if not os.path.exists( BOOTSTRAP_NODE_FILE ):
      rc = bootstrap_node()
      if rc != 0:
         return rc

   # install Syndicate
   if os.path.exists( SYNDICATE_ROOT ):
      rc = sh([ "svn", "up", SYNDICATE_ROOT])
      if rc != 0:
         log( "ERROR", "bootstrap_client: build clean rc = %s" % rc )
         return rc

   else:
      rc = sh([ "svn", "checkout", "https://svn.princeton.edu/cdnfs", SYNDICATE_ROOT])
      if rc != 0:
         log( "ERROR", "bootstrap_client: 'svn checkout' rc = %s" % rc )
         return rc

   rc = make( target )
   if rc != 0:
      return rc

   log( "INFO", "successfully bootstrapped client" )
   return 0

if __name__ == "__main__":
   log( "INFO", "begin bootstrap node")
   rc = bootstrap( TARGET )
   log( "INFO", "bootstrap_client rc = %s" % rc)
   
