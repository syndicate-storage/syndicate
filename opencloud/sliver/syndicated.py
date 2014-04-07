#!/usr/bin/python

"""
Daemon that sits on a sliver, and securely receives commands from the Syndicate OpenCloud observer
to set up or tear down access to a Volume.

This daemon will provision gateways at OpenCloud's request.
"""

import os
import sys
import signal
import resource
import daemon 
import grp
import lockfile
import argparse

import logging
from logging import Logger
log = logging.getLogger()
log.setLevel( logging.INFO )

import syndicate
import syndicate.client.bin.syntool as syntool
import syndicate.client.common.msconfig as msconfig

#-------------------------------
CONFIG_OPTIONS = {
   "config":            ("-c", 1, "Path to the daemon configuration file"),
   "cache":             ("-k", 1, "Directory to cache metadata from OpenCloud"),
   "opencloud":         ("-o", 1, "URL to the OpenCloud SMI"),
   "foreground":        ("-f", 0, "Run in the foreground"),
   "logdir":            ("-L", 1, "Directory to contain the log files.  If not given, then write to stdout and stderr."),
   "pidfile":           ("-l", 1, "Path to the desired PID file.")
}

DEFAULT_CONFIG = {
    "config":   "/etc/syndicated.conf",
    "cache":    "/var/cache/syndicated",
    "logdir":   "/var/log/syndicated",
    "pidfile":  "/var/run/syndicated.pid",
}

#-------------------------------
def connect_syndicate():
    """
    Connect to the OpenCloud Syndicate SMI
    """
    client = syntool.Client( CONFIG.SYNDICATE_OPENCLOUD_USER, CONFIG.SYNDICATE_SMI_URL,
                             password=CONFIG.SYNDICATE_OPENCLOUD_PASSWORD,
                             debug=True )

    return client


#-------------------------------
def make_gateway_name( gateway_type, volume_name, host ):
    """
    Generate a name for a gateway
    """
    return "OpenCloud-%s-%s-%s" % (volume_name, gateway_type, host)    


#-------------------------------
def ensure_gateway_exists( gateway_type, user_email, volume_name, host, port, closure=None ):
    """
    Ensure that a particular type of gateway with the given fields exists.
    Create one if need be.
    We assume that the Volume (and thus user) already exist...if they don't, its an error.
    """

    client = connect_syndicate()
    
    gateway_name = make_gateway_name( gateway_type, volume_name, host )

    try:
        gateway = client.read_gateway( gateway_name )
    except Exception, e:
        # transport error 
        logger.exception(e)
        raise e

    need_create_gateway = False

    # is it the right gateway?
    if gateway is not None:
        
        # the associated user and volume must exist, and they must match 
        try:
            user = client.read_user( user_email )
        except Exception, e:
            # transport error
            logger.exception(e)
            raise e

        try:
            volume = client.read_volume( volume_name )
        except Exception, e:
            # transport error 
            logger.exception(e)
            raise e

        # these had better exist...
        if user is None or volume is None:
            raise Exception("Orphaned gateway with the same name as us (%s)" % gateway_name)

        # does this gateway match the user and volume it claims to belong to?
        # NOTE: this doesn't check the closure!
        if msconfig.GATEWAY_TYPE_TO_STR[ gateway["gateway_type"] ] != gateway_type or gateway['owner_id'] != user['owner_id'] or gateway['volume_id'] != volume['volume_id']:
            raise Exception("Gateway exists under a different volume (%s) or user (%s)" % (volume['name'], user['email']))

        # gateway exists, and is owned by the given volume and user 
        return gateway

    else:
        # create the gateway 
        kw = {}
        if closure is not None:
            kw['closure'] = closure

        
        try:
            gateway = client.create_gateway( volume_name, user_email, gateway_type, gateway_name, host, port, **kw )
        except Exception, e:
            # transport, collision, or missing Volume or user
            logger.exception(e)
            raise e

        else:
            return gateway


#-------------------------------
def ensure_gateway_absent( gateway_type, user_email, volume_name, host ):
    """
    Ensure that a particular gateway does not exist.
    """
    
    client = connect_syndicate()
    gateway_name = make_gateway_name( gateway_type, volume_name, host )

    client.delete_gateway( gateway_name )
    return True


#-------------------------------
def ensure_UG_exists( user_email, volume_name, host, port=32780 ):
    """
    Ensure that a particular UG exists.
    """
    return ensure_gateway_exists( "UG", user_email, volume_name, host, port )


#-------------------------------
def ensure_RG_exists( user_email, volume_name, host, port=32800, closure=None ):
    """
    Ensure that a particular RG exists.
    """
    return ensure_gateway_exists( "RG", user_email, volume_name, host, port, closure=closure )


#-------------------------------
def ensure_UG_absent( user_email, volume_name, host ):
    """
    Ensure that a particular UG does not exist
    """
    return ensure_gateway_absent( "UG", user_email, volume_name, host )


#-------------------------------
def ensure_UG_absent( user_email, volume_name, host ):
    """
    Ensure that a particular RG does not exist
    """
    return ensure_gateway_absent( "RG", user_email, volume_name, host )


#-------------------------------
def daemonize( config, main_method ):
   """
   Become a daemon.  Run our main method.
   """
   signal_map = {
      signal.SIGTERM: 'terminate',
      signal.SIGHUP: None
   }
   
   daemon_gid = grp.getgrnam('daemon').gr_gid
   
   output_fd = None
   error_fd = None
   
   if config.get("logdir", None) != None:
      # create log files
      # TODO: access.log, error.log
      output_fd = open( os.path.join(config["logdir"], "syndicated.log"), "w+" )
      error_fd = output_fd 
      
      os.dup2( output_fd, sys.stdout )
      os.dup2( output_fd, sys.stderr )
      
   else:
      # write to stdout and stderr
      output_fd = sys.stdout 
      error_fd = sys.stderr
   
   context = daemon.DaemonContext( umask=0o002, prevent_core=True, signal_map=signal_map )
   
   # don't close these if they're open...
   files_preserve = []
   if output_fd:
      files_preserve.append(output_fd)
   if error_fd and error_fd != output_fd:
      files_preserve.append(error_fd)
   
   context.files_preserve = files_preserve
   
   # pid file?
   pidfile_path = config.get("pidfile", None) 
   if pidfile_path:
      context.pidfile = lockfile.FileLock(pidfile_path)
   
   # start up
   with context:
      main_method()


#-------------------------
def load_config( config_str, opts ):
   
   config = None 
   
   if config_str:
      config = ConfigParser.SafeConfigParser()
      config_fd = StringIO.StringIO( config_str )
      config_fd.seek( 0 )
      
      try:
         config.readfp( config_fd )
      except Exception, e:
         log.exception( e )
         return None
   
   ret = {}
   ret["_in_argv"] = []
   ret["_in_config"] = []
   
   # convert to dictionary, merging in argv opts
   for arg_opt in CONFIG_OPTIONS.keys():
      if hasattr(opts, arg_opt) and getattr(opts, arg_opt) != None:
         ret[arg_opt] = getattr(opts, arg_opt)
         
         # force singleton...
         if isinstance(ret[arg_opt], list) and len(ret[arg_opt]) == 1 and CONFIG_OPTIONS[arg_opt][1] == 1:
            ret[arg_opt] = ret[arg_opt][0]
            
         ret["_in_argv"].append( arg_opt )
      
      elif config != None and config.has_option("syndicated", arg_opt):
         ret[arg_opt] = config.get("syndicated", arg_opt)
         
         ret["_in_config"].append( arg_opt )
   
   return ret

#-------------------------
def build_parser( progname ):
   parser = argparse.ArgumentParser( prog=progname, description="Syndicate control daemon" )
   
   for (config_option, (short_option, nargs, config_help)) in CONFIG_OPTIONS.items():
      if not isinstance(nargs, int) or nargs >= 1:
         if short_option:
            # short option means 'typical' argument
            parser.add_argument( "--" + config_option, short_option, metavar=config_option, nargs=nargs, help=config_help)
         else:
            # no short option (no option in general) means accumulate
            parser.add_argument( config_option, metavar=config_option, type=str, nargs=nargs, help=config_help)
      else:
         # no argument, but mark its existence
         parser.add_argument( "--" + config_option, short_option, action="store_true", help=config_help)
   
   return parser


#-------------------------
def validate_args( config ):
   
   # required arguments
   required = ['opencloud']
   for req in required:
      if config.get( req, None ) == None:
         raise Exception("Missing required argument: %s" % req )
   
   return True
      
#-------------------------
def build_config( argv ):
   
   parser = build_parser( argv[0] )
   opts = parser.parse_args( argv[1:] )
   config = load_config( None, opts )
   
   if config == None:
      log.error("Failed to load configuration")
      parser.print_help()
      sys.exit(1)
   
   rc = validate_args( config )
   if not rc:
      log.error("Invalid arguments")
      parser.print_help()
      sys.exit(1)
      
   return config
      
#-------------------------
def main( config ):
   
   # get our hostname
   hostname = socket.gethostname()
   
   # start waiting for commands from OpenCloud
   # TODO
   
   return True

#-------------------------    
if __name__ == "__main__":
   config = build_config( argv )
   
   if config.get("foreground", None):
        main( config )
   else:
        daemonize( config, lambda: main(config) )
        

