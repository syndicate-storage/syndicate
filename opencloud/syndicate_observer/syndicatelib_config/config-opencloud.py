#!/usr/bin/python

# ---------------------------------
# This is the configuration file used by the Syndicate observer.
# It is a well-formed Python file, and will be imported into the
# observer as a Python module.  This means you can run any config-
# generation code here you like, but all of the following global 
# variables must be defined.
# ---------------------------------

# URL to the Syndicate SMI.  For example, https://syndicate-metadata.appspot.com
SYNDICATE_SMI_URL="http://localhost:8080"

# If you are going to use OpenID to authenticate the Syndicate sliver daemon,
# this is the OpenID provider URL.  It is currently used only to generate 
# identity pages for users, so you can put whatever you want here for now.
SYNDICATE_OPENID_TRUSTROOT="http://localhost:8081"

# This is the observer's user account on Syndicate.  You must create it out-of-band
# prior to using the observer, and it must be an admin user since it will
# create other users (i.e. for slices).
SYNDICATE_OPENCLOUD_USER="jcnelson@cs.princeton.edu"

# This is the password for the observer to authenticate itself to Syndicate.
SYNDICATE_OPENCLOUD_PASSWORD="nya"

# If the observer uses public-key authentication with Syndicate, you will 
# need to identify the absolute path to its private key here.  It must be 
# a 4096-bit PEM-encoded RSA key, and the Syndicate observer's user account
# must have been given the public key on activation.
SYNDICATE_OPENCLOUD_PKEY=None

# This is the location on disk where Syndicate observer code can be found, 
# if it is not already in the Python path.  This is optional.
SYNDICATE_PYTHONPATH="/root/syndicate/build/out/python"

# This is the location of the observer's private key.  It must be an absolute
# path, and refer to a 4096-bit PEM-encoded RSA key.
SYNDICATE_PRIVATE_KEY="/opt/planetstack/syndicate_observer/syndicatelib_config/pollserver.pem"

# This is the master secret used to generate secrets to seal sensitive information sent to the 
# Syndicate sliver mount daemons.  It is also used to seal sensitive information
# stored to the Django database.  
# TODO: think of a way to not have to store this on disk.  Maybe we feed into the
# observer when it starts up?
SYNDICATE_OPENCLOUD_SECRET="e4988309a5005edb8ea185f16f607938c0fb7657e4d7609853bcb7c4884d1c92"

# This is the default port number on which a Syndicate Replica Gateway
# will be provisioned.  It's a well-known port, and can be the same across
# slivers, since in OpenCloud, an RG instance only listens to localhost.
SYNDICATE_RG_DEFAULT_PORT=38800

# This is the absolute path to the RG's storage driver (which will be automatically
# pushed to slivers by Syndicate).  See https://github.com/jcnelson/syndicate/wiki/Replica-Gateways
SYNDICATE_RG_CLOSURE=None

# This is the port number the observer listens on for GETs from the Syndicate sliver mount 
# daemons.  Normally, the oserver pushes (encrypted) commands to the daemons, but if the 
# daemons are NAT'ed or temporarily partitioned, they will pull commands instead.
SYNDICATE_HTTP_PORT=65321

# This is the port number the Syndicate sliver mount daemons listen on.  The observer will 
# push commands to them on this port.
SYNDICATE_SLIVER_PORT=65322

# If true, print verbose debug messages.
DEBUG=True
