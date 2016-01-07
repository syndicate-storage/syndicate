#!/usr/bin/python 

"""
   Copyright 2014 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import sys
import signal
import argparse
import cgi
import BaseHTTPServer
import base64
import json 
import errno
import requests
import threading
import psutil
import socket
import subprocess
import shlex
import time
import copy
import binascii

from Crypto.Hash import SHA256 as HashAlg
from Crypto.PublicKey import RSA as CryptoKey
from Crypto import Random
from Crypto.Signature import PKCS1_PSS as CryptoSigner

import logging
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
log = logging.getLogger()
log.setLevel( logging.INFO )

import syndicate

import syndicate.ms.syntool as syntool 

import syndicate.util.watchdog as watchdog
import syndicate.util.provisioning as provisioning

import syndicate.observer.cred as observer_cred

# watchdog names
SYNDICATE_UG_WATCHDOG_NAME              = "syndicate-ug"
SYNDICATE_RG_WATCHDOG_NAME              = "syndicate-rg"
SYNDICATE_AG_WATCHDOG_NAME              = "syndicate-ag"

#-------------------------------
def make_UG_argv( program, syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint, hostname=None, debug=False ):
   # NOTE: run in foreground; watchdog handles the rest
   hostname_str = ""
   if hostname is not None:
      hostname_str = "-H %s" % hostname 
      
   debug_str = ""
   if debug:
      debug_str = "-d2"
      
   return "%s -f %s -m %s -u %s -v %s -g %s -K %s -P '%s' %s %s" % (program, debug_str, syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, hostname_str, mountpoint )


#-------------------------------
def make_RG_argv( program, syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, hostname=None, debug=False ):
   hostname_str = ""
   if hostname is not None:
      hostname_str = "-H %s" % hostname 
      
   debug_str = ""
   if debug:
      debug_str = "-d2"
      
   return "%s %s -m %s -u %s -v %s -g %s -K %s -P '%s' %s" % (program, debug_str, syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, hostname_str)


#-------------------------------
def start_UG( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint, uid_name=None, gid_name=None, hostname=None, debug=False ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information on argv,
   # which should NOT become visible to other users via /proc
   
   command_str = make_UG_argv( SYNDICATE_UG_WATCHDOG_NAME, syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint, hostname=hostname, debug=debug )
   
   log.info("Starting UG (%s)" % SYNDICATE_UG_WATCHDOG_NAME )
   
   # start the watchdog
   pid = watchdog.run( SYNDICATE_UG_WATCHDOG_NAME, [SYNDICATE_UG_WATCHDOG_NAME, '-v', volume_name, '-m', mountpoint], command_str, uid_name=uid_name, gid_name=gid_name )
   
   if pid < 0:
      log.error("Failed to make UG watchdog %s, rc = %s" % (SYNDICATE_UG_WATCHDOG_NAME, pid))
   
   return pid
   

#-------------------------------
def start_RG( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, uid_name=None, gid_name=None, hostname=None, debug=False ):
   # generate the command, and pipe it over
   # NOTE: do NOT execute the command directly! it contains sensitive information on argv,
   # which should NOT become visible to other users via /proc
   
   command_str = make_RG_argv( SYNDICATE_RG_WATCHDOG_NAME, syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, hostname=hostname, debug=debug )
   
   log.info("Starting RG (%s)" % SYNDICATE_RG_WATCHDOG_NAME )
   
   # start the watchdog
   pid = watchdog.run( SYNDICATE_RG_WATCHDOG_NAME, [SYNDICATE_RG_WATCHDOG_NAME, '-R', '-v', volume_name], command_str, uid_name=uid_name, gid_name=gid_name )
   
   if pid < 0:
      log.error("Failed to make RG watchdog %s, rc = %s" % (SYNDICATE_RG_WATCHDOG_NAME, pid))
   
   return pid
   

#-------------------------------
def stop_gateway_watchdog( pid ):
   # stop a watchdog, given a PID.
   # return 0 on success, -1 on error
   
   # tell the watchdog to die, so it shuts down the UG
   try:
      os.kill( pid, signal.SIGTERM )
   except OSError, oe:
      if oe.errno != errno.ESRCH:
         # NOT due to the process dying after we checked for it
         log.exception(oe)
         return -1
   
   except Exception, e:
      log.exception(e)
      return -1
   
   return 0


#-------------------------------
def stop_UG( volume_name, mountpoint=None ):
   # stop a UG, given its mountpoint and volume name
   # this method is idempotent
   
   query_attrs = { "volume": volume_name }
   
   if mountpoint is not None:
      query_attrs["mountpoint"] = mountpoint
   
   mounted_UGs = watchdog.find_by_attrs( SYNDICATE_UG_WATCHDOG_NAME, query_attrs )
   if len(mounted_UGs) > 0:
      for proc in mounted_UGs:
         rc = stop_gateway_watchdog( proc.pid )
         if rc != 0:
            return rc
   
   return 0


#-------------------------------
def stop_RG( volume_name ):
   # stop an RG
   running_RGs = watchdog.find_by_attrs( SYNDICATE_RG_WATCHDOG_NAME, {"volume": volume_name} )
   if len(running_RGs) > 0:
      for proc in running_RGs:
         rc = stop_gateway_watchdog( proc.pid )
         if rc != 0:
            return rc
         
   return 0

         
#-------------------------------
def ensure_UG_running( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint=None, check_only=False, uid_name=None, gid_name=None, hostname=None, debug=False ):
    """
    Ensure that a User Gateway is running on a particular mountpoint.
    Return 0 on success
    Return negative on error.
    """
    
    if mountpoint is None:
       log.error("Missing mountpout.  Pass mountpoint=...")
       return -errno.EINVAL
    
    # make sure a mountpoint exists
    rc = ensure_UG_mountpoint_exists( mountpoint, uid_name=uid_name, gid_name=gid_name )
    if rc != 0:
       log.error("Failed to ensure mountpoint %s exists" % mountpoint)
       return rc
    
    # is there a UG running at this mountpoint?
    mounted_UGs = watchdog.find_by_attrs( SYNDICATE_UG_WATCHDOG_NAME, {"volume": volume_name, "mountpoint": mountpoint} )
    if len(mounted_UGs) == 1:
       # we're good!
       logging.info("UG for %s at %s already running; PID = %s" % (volume_name, mountpoint, mounted_UGs[0].pid))
       return mounted_UGs[0].pid
    
    elif len(mounted_UGs) > 1:
       # too many!  probably in the middle of starting up 
       logging.error("Multiple UGs running for %s on %s...?" % (volume_name, mountpoint))
       return -errno.EAGAN
    
    else: 
       logging.error("No UG running for %s on %s" % (volume_name, mountpoint))
       if not check_only:
          pid = start_UG( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, mountpoint, uid_name=uid_name, gid_name=gid_name, hostname=hostname, debug=debug )
          if pid < 0:
             log.error("Failed to start UG in %s at %s, rc = %s" % (volume_name, mountpoint, pid))
          
          return pid
       
       else:
          return 0


#-------------------------
def check_UG_mounted( mountpoint, fstype=None ):
   """
   See if a UG is mounted, by walking /proc/mounts
   """
   
   fd = None
   mounts = None
   
   try:
      fd = open("/proc/mounts", "r")
      mounts = fd.read()
      fd.close()
   except IOError, ie:
      logging.error("Failed to read /proc/mounts, errno = %s" % ie.errno )
      return -ie.errno
   except OSError, oe:
      logging.error("Failed to read /proc/mounts, errno = %s" % oe.errno )
      return -oe.errno 
   finally:
      if fd is not None:
         fd.close()
         fd = None
   
   mount_lines = mounts.strip().split("\n")
   for mount in mount_lines:
      # format: FS MOUNTPOINT ...
      mount_parts = mount.split()
      mount_fstype = mount_parts[2]
      mount_dir = mount_parts[1]
      
      if mount_dir.rstrip("/") == mountpoint.rstrip("/"):
         # something's mounted here...
         if fstype is not None:
            if fstype == mount_fstype:
               return True
            else:
               # something else is mounted here 
               return False 
            
         else:
            # we don't care about the fstype 
            return True 
         
   # nothing mounted here 
   return False
         

#-------------------------
def ensure_UG_not_mounted( mountpoint, UG_fstype=None ):
   """
   Ensure that a directory does not have a UG running on it.
   Return 0 on success, negative otherwise
   """
   if not os.path.exists( mountpoint ):
      return True
   
   mounted = check_UG_mounted( mountpoint, fstype=UG_fstype )
   
   if mounted:
      # try unmounting 
      rc = subprocess.call(["/bin/fusermount", "-u", mountpoint], stderr=None )
      
      if rc != 0:
         # fusermount failed...
         logging.error("Failed to unmount %s, fusermount exit status %s" % (mountpoint, rc))
         return -errno.EPERM 
      
      else:
         # verify unmounted 
         mounted = check_UG_mounted( mountpoint, fstype=UG_fstype )
         
         if not mounted:
            # failed to unmount
            logging.error("Failed to unmount %s")
            return -errno.EAGAIN 
   
   return 0


#-------------------------------
def ensure_UG_stopped( volume_name, mountpoint=None, UG_fstype=None ):
    """
    Ensure a UG is no longer running.
    """
    
    # stop the process
    rc = stop_UG( volume_name, mountpoint=mountpoint )
    if rc != 0:
       log.error("Failed to stop UG in %s at %s, rc = %s" % (volume_name, mountpoint, rc))
    
    if mountpoint is not None:
      
      # ensure it's not mounted
      rc = ensure_UG_not_mounted( mountpoint, UG_fstype=UG_fstype )
      if rc != 0:
         logging.error("Failed to ensure UG is not mounted on %s, rc = %s" % (mountpoint, rc))
         return rc
      
      # remove the directory
      ensure_UG_mountpoint_absent( mountpoint )
      
    return rc


#-------------------------------
def ensure_RG_running( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, check_only=False, uid_name=None, gid_name=None, hostname=None, debug=False ):
    """
    Ensure an RG is running.  Return the PID on success.
    """
    
    # is there an RG running for this volume?
    running_RGs = watchdog.find_by_attrs( SYNDICATE_RG_WATCHDOG_NAME, {"volume": volume_name} )
    if len(running_RGs) == 1:
       # we're good!
       logging.info("RG for %s already running; PID = %s" % (volume_name, running_RGs[0].pid))
       return running_RGs[0].pid
    
    elif len(running_RGs) > 1:
       # too many! probably in the middle of starting up 
       logging.error("Multiple RGs running for %s...?" % (volume_name))
       return -errno.EAGAIN
    
    else:
       logging.error("No RG running for %s" % (volume_name))
       if not check_only:
          pid = start_RG( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, uid_name=uid_name, gid_name=gid_name, hostname=hostname, debug=debug )
          if pid < 0:
             log.error("Failed to start RG in %s, rc = %s" % (volume_name, pid))
             
          return pid
                       
       else:
          # not running
          return -errno.ENOENT
       

#-------------------------------
def ensure_RG_stopped( volume_name ):
    """
    Ensure that the RG is stopped.
    """
    rc = stop_RG( volume_name )
    if rc != 0:
       log.error("Failed to stop RG in %s, rc = %s" % (volume_name, rc))

    return rc


#-------------------------------
def ensure_AG_running( syndicate_url, principal_id, volume_name, gateway_name, key_password, user_pkey_pem, check_only=False, uid_name=None, gid_name=None, hostname=None, debug=False ):
   # TODO 
   pass 


#-------------------------------
def ensure_AG_stopped( volume_name ):
   # TODO
   pass 

   
#-------------------------------
def make_UG_mountpoint_path( mountpoint_dir, volume_name ):
    """
    Generate the path to a mountpoint.
    """
    vol_dirname = volume_name.replace("/", ".")
    vol_mountpoint = os.path.join( mountpoint_dir, vol_dirname )
    return vol_mountpoint


#-------------------------------
def ensure_UG_mountpoint_exists( mountpoint, uid_name=None, gid_name=None ):
   """
   Make a mountpoint (i.e. a directory)
   """
   rc = 0
   try:
      os.makedirs( mountpoint, mode=0777 )
      
      if uid_name is not None and gid_name is not None:
         os.system("chown %s.%s %s" % (uid_name, gid_name, mountpoint))
         
      return 0
   except OSError, oe:
      if oe.errno != errno.EEXIST:
         return -oe.errno
      else:
         return 0
   except Exception, e:
      log.exception(e)
      return -errno.EPERM   

#-------------------------
def ensure_UG_mountpoint_absent( mountpoint ):
   """
   Ensure that a mountpoint no longer exists 
   """
   try:
      os.rmdir( mountpoint )
   except OSError, oe:
      if oe.errno != errno.ENOENT:
         log.error("Failed to remove unused mountpoint %s, errno = %s" % (mountpoint, oe.errno))
   
   except IOError, ie:
      if ie.errno != errno.ENOENT:
         log.error("Failed to remove unused mountpoint %s, errno = %s" % (mountpoint, ie.errno))
         
   

#-------------------------
def list_running_gateways_by_volume():
   """
   Find the set of running gateways, grouped by volume.
   
   return a dictionary with the structure of:
      { volume_name : { gateway_type: { "pids": [gateway_pid] } } }
   """
   
   watchdog_names = {
      "UG": SYNDICATE_UG_WATCHDOG_NAME,
      "RG": SYNDICATE_RG_WATCHDOG_NAME,
      "AG": SYNDICATE_AG_WATCHDOG_NAME
   }
   
   watchdog_name_to_type = dict( [(v, k) for (k, v) in watchdog_names.items()] )
   
   ret = {}
   
   for gateway_type in ["UG", "RG", "AG"]:
      
      watchdog_name = watchdog_names[ gateway_type ]
      
      running_watchdog_procs = watchdog.find_by_attrs( watchdog_name, {} )
      
      # from these, find out which volumes
      for running_watchdog_proc in running_watchdog_procs:
         
         cmdline = watchdog.get_proc_cmdline( running_watchdog_proc )[0]
         
         watchdog_attrs = watchdog.parse_proc_attrs( cmdline )
         
         # find the volume name 
         volume_name = watchdog_attrs.get("volume", None)
         
         if volume_name is None:
            # nothing to do
            continue
      
         if not ret.has_key( volume_name ):
            # add volume record 
            ret[volume_name] = {}
         
         if not ret[volume_name].has_key( gateway_type ):
            # add gateway record 
            ret[volume_name][gateway_type] = {}
         
         if not ret[volume_name][gateway_type].has_key( "pids" ):
            # add pids list 
            ret[volume_name][gateway_type][pids] = []
         
         ret[volume_name][gateway_type]["pids"].append( running_watchdog_proc.pid )
         
         
   return ret
      

#-------------------------
def gateway_directives_from_volume_info( volume_info, local_hostname, slice_secret ):
   """
   Extract gateway directives from an observer's description of the volume for this host.
   """
   
   gateway_directives = {
      "UG": {},
      "RG": {},
      "AG": {}
   }
   
   
   volume_name = volume_info[ observer_cred.OPENCLOUD_VOLUME_NAME ]
   gateway_name_prefix = volume_info[ observer_cred.OPENCLOUD_SLICE_GATEWAY_NAME_PREFIX ]
   
   # get what we need...
   try:
         
      RG_hostname = local_hostname
      AG_hostname = local_hostname 
      
      # global hostnames (i.e. multiple instantiations of the same gateway) override local hostnames.
      if volume_info[ observer_cred.OPENCLOUD_SLICE_AG_GLOBAL_HOSTNAME ] is not None:
         AG_hostname = volume_info[ observer_cred.OPENCLOUD_SLICE_AG_GLOBAL_HOSTNAME ]
      
      if volume_info[ observer_cred.OPENCLOUD_SLICE_RG_GLOBAL_HOSTNAME ] is not None:
         RG_hostname = volume_info[ observer_cred.OPENCLOUD_SLICE_RG_GLOBAL_HOSTNAME ]
      
      gateway_directives["UG"]["instantiate"]   = volume_info[ observer_cred.OPENCLOUD_SLICE_INSTANTIATE_UG ]
      gateway_directives["UG"]["run"]           = volume_info[ observer_cred.OPENCLOUD_SLICE_RUN_UG ]
      gateway_directives["UG"]["port"]          = volume_info[ observer_cred.OPENCLOUD_SLICE_UG_PORT ]
      gateway_directives["UG"]["closure"]       = volume_info[ observer_cred.OPENCLOUD_SLICE_UG_CLOSURE ]
      gateway_directives["UG"]["name"]          = provisioning.make_gateway_name( gateway_name_prefix, "UG", volume_name, local_hostname )
      gateway_directives["UG"]["key_password"]  = provisioning.make_gateway_private_key_password( gateway_directives["UG"]["name"], slice_secret )
      gateway_directives["UG"]["hostname"]      = local_hostname
      
      gateway_directives["RG"]["instantiate"]   = volume_info[ observer_cred.OPENCLOUD_SLICE_INSTANTIATE_RG ]
      gateway_directives["RG"]["run"]           = volume_info[ observer_cred.OPENCLOUD_SLICE_RUN_RG ]
      gateway_directives["RG"]["port"]          = volume_info[ observer_cred.OPENCLOUD_SLICE_RG_PORT ]
      gateway_directives["RG"]["closure"]       = volume_info[ observer_cred.OPENCLOUD_SLICE_RG_CLOSURE ]
      gateway_directives["RG"]["name"]          = provisioning.make_gateway_name( gateway_name_prefix, "RG", volume_name, RG_hostname )
      gateway_directives["RG"]["key_password"]  = provisioning.make_gateway_private_key_password( gateway_directives["RG"]["name"], slice_secret )
      gateway_directives["RG"]["hostname"]      = RG_hostname
      
      gateway_directives["AG"]["instantiate"]   = volume_info[ observer_cred.OPENCLOUD_SLICE_INSTANTIATE_AG ]
      gateway_directives["AG"]["run"]           = volume_info[ observer_cred.OPENCLOUD_SLICE_RUN_AG ]
      gateway_directives["AG"]["port"]          = volume_info[ observer_cred.OPENCLOUD_SLICE_AG_PORT ]
      gateway_directives["AG"]["closure"]       = volume_info[ observer_cred.OPENCLOUD_SLICE_AG_CLOSURE ]
      gateway_directives["AG"]["name"]          = provisioning.make_gateway_name( gateway_name_prefix, "AG", volume_name, AG_hostname )
      gateway_directives["AG"]["key_password"]  = provisioning.make_gateway_private_key_password( gateway_directives["AG"]["name"], slice_secret )
      gateway_directives["AG"]["hostname"]      = AG_hostname
      
   except Exception, e:
      log.exception(e)
      log.error("Invalid configuration for Volume %s" % volume_name)
      return None 
   
   return gateway_directives

   

#-------------------------
def apply_instantion_and_runchange( gateway_directives, inst_funcs, runchange_funcs ):
   """
   Apply instantiation and runchage functions over gateways, based on observer directives.
   inst_funcs must be a dict of {"gateway_type" : callable(bool)} that changes the instantiation of the gateway.
   runchage_funcs must be a dict of {"gateway_type" : callable(bool)} that changes the running status of a gateway.
   """
   
   # run alloc functions
   for gateway_type in ["UG", "RG", "AG"]:
      
      try:
         
         gateway_name = gateway_directives[ gateway_type ][ "name" ]
         instantiation_status = gateway_directives[ gateway_type ][ "instantiate" ]
         
         rc = inst_funcs[ gateway_type ]( instantiation_status )
         
         assert rc is not None, "Failed to set instantiation = %s for %s %s with %s, rc = %s" % (instantiation_status, gateway_type, gateway_name, inst_funcs[ gateway_type ], rc )
         
      except Exception, e:
         log.exception(e)
         return -errno.EPERM 
   
   
   # run runchange funcs 
   for gateway_type in ["UG", "RG", "AG"]:
      
      try:
         
         gateway_name = gateway_directives[ gateway_type ][ "name" ]
         run_status = gateway_directives[ gateway_type ][ "run" ]
         
         rc = runchange_funcs[ gateway_type ]( run_status )
         
         assert rc == 0, "Failed to set running = %s for %s %s with %s, rc = %s" % (run_status, gateway_type, gateway_name, runchange_funcs[ gateway_type ], rc)
         
      except Exception, e:
         log.exception(e)
         return -errno.EPERM
      
      
   return 0


#-------------------------
def start_stop_volume( config, volume_info, slice_secret, client=None, hostname=None, gateway_uid_name=None, gateway_gid_name=None, debug=False ):
   """
   Ensure that the instantiation and run status of the gateways for a volume match what the observer thinks it is.
   This method is idempotent.
   """
   
   volume_name = volume_info[ observer_cred.OPENCLOUD_VOLUME_NAME ]

   # get what we need...
   try:
      syndicate_url             = volume_info[ observer_cred.OPENCLOUD_SYNDICATE_URL ]
      principal_id              = volume_info[ observer_cred.OPENCLOUD_VOLUME_OWNER_ID ]
      principal_pkey_pem        = volume_info[ observer_cred.OPENCLOUD_PRINCIPAL_PKEY_PEM ]
      
   except:
      log.error("Invalid configuration for Volume %s" % volume_name)
      return -errno.EINVAL
   
   
   if client is None:
      # connect to syndicate 
      client = syntool.Client( principal_id, syndicate_url, user_pkey_pem=principal_pkey_pem, debug=config['debug'] )
   
   mountpoint_dir = config['mountpoint_dir']
   UG_mountpoint_path = make_UG_mountpoint_path( mountpoint_dir, volume_name )
   
   volume_name = volume_info[ observer_cred.OPENCLOUD_VOLUME_NAME ]
   
   if hostname is None:
      hostname = socket.gethostname()
   
   # build up the set of directives
   gateway_directives = gateway_directives_from_volume_info( volume_info, hostname, slice_secret )
   
   rc = apply_gateway_directives( client, syndicate_url, principal_id, principal_pkey_pem, volume_name, gateway_directives, UG_mountpoint_path,
                                  gateway_uid_name=gateway_uid_name, gateway_gid_name=gateway_gid_name, debug=debug )
   
   if rc != 0:
      log.error("Failed to apply gateway directives to synchronize %s, rc = %s" % (volume_name, rc))
      
   return rc

   
#-------------------------
def apply_gateway_directives( client, syndicate_url, principal_id, principal_pkey_pem, volume_name, gateway_directives, UG_mountpoint_path,
                              gateway_uid_name=None, gateway_gid_name=None, debug=False ):
   """
   Apply the set of gateway directives.
   """
   
   # functions that instantiate gateways.
   # NOTE: they all take the same arguments, so what we're about to do is totally valid
   inst_funcs_to_type = {
      "UG": provisioning.ensure_UG_exists,
      "RG": provisioning.ensure_RG_exists,
      "AG": provisioning.ensure_AG_exists
   }
   
   
   # inner function for instantiaing a gateway
   def _gateway_inst_func( gateway_type, should_instantiate ):
      
      log.info("Switch %s for %s to instantiation '%s'" % (gateway_type, volume_name, should_instantiate))
      
      if should_instantiate == True:
         new_gateway = inst_funcs_to_type[gateway_type]( client,
                                                         principal_id,
                                                         volume_name,
                                                         gateway_directives[gateway_type]["name"],
                                                         gateway_directives[gateway_type]["hostname"],
                                                         gateway_directives[gateway_type]["port"],
                                                         gateway_directives[gateway_type]["key_password"] )
         
         if new_gateway is not None:
            return 0
         else:
            return -errno.EPERM
         
      elif should_instantiate == False:
         rc = provisioning.ensure_gateway_absent( client, gateway_directives[gateway_type]["name"] )
         
         if rc == True:
            return 0 
         else:
            return -errno.EPERM
         
      else:
         return 0
      
   
   # construct partially-evaluated instantiation functions 
   inst_funcs = {
      "UG": lambda should_instantiate: _gateway_inst_func( "UG", should_instantiate ),
      "RG": lambda should_instantiate: _gateway_inst_func( "RG", should_instantiate ),
      "AG": lambda should_instantiate: _gateway_inst_func( "AG", should_instantiate )
   }   
   
   # inner function for ensuring a UG is running 
   def _runchange_UG( should_run ):
      
      log.info("Switch UG for %s to run status '%s'" % (volume_name, should_run))
      
      if should_run == True:
         rc = ensure_UG_running( syndicate_url,
                                 principal_id,
                                 volume_name,
                                 gateway_directives["UG"]["name"],
                                 gateway_directives["UG"]["key_password"],
                                 principal_pkey_pem,
                                 mountpoint=UG_mountpoint_path,
                                 check_only=False,
                                 uid_name=gateway_uid_name,
                                 gid_name=gateway_gid_name,
                                 hostname=gateway_directives['UG']['hostname'],
                                 debug=debug )
         
         if rc < 0:
            return rc
         else:
            return 0
      
      elif should_run == False:
         return ensure_UG_stopped( volume_name, mountpoint=UG_mountpoint_path )
      
      else:
         return 0
      
      
   # inner function for ensuring an RG is running 
   def _runchange_RG( should_run ):
      
      log.info("Switch RG for %s to run status '%s'" % (volume_name, should_run))
      
      if should_run == True:
         rc = ensure_RG_running( syndicate_url,
                                 principal_id,
                                 volume_name,
                                 gateway_directives["RG"]["name"],
                                 gateway_directives["RG"]["key_password"],
                                 principal_pkey_pem,
                                 check_only=False,
                                 uid_name=gateway_uid_name,
                                 gid_name=gateway_gid_name,
                                 hostname=gateway_directives['RG']['hostname'],
                                 debug=debug )
         
         if rc < 0:
            return rc
         else:
            return 0
      
      elif should_run == False:
         return ensure_RG_stopped( volume_name )
      
      else:
         return 0
   
   
   # inner function for ensuring an RG is running 
   def _runchange_AG( should_run ):
      
      log.info("Switch RG for %s to run status '%s'" % (volume_name, should_run))
      
      if should_run == True:
         rc = ensure_AG_running( syndicate_url,
                                 principal_id,
                                 volume_name,
                                 gateway_directives["AG"]["name"],
                                 gateway_directives["AG"]["key_password"],
                                 principal_pkey_pem,
                                 check_only=False,
                                 uid_name=gateway_uid_name,
                                 gid_name=gateway_gid_name,
                                 hostname=gateway_directives['AG']['hostname'],
                                 debug=debug )
         
         if rc < 0:
            return rc
         else:
            return 0
      
      elif should_run == False:
         return ensure_AG_stopped( volume_name )
      
      else:
         return 0
      
      
   # functions that start gateways
   runchange_funcs = {
      "UG": lambda should_run: _runchange_UG( should_run ),
      "RG": lambda should_run: _runchange_RG( should_run ),
      "AG": lambda should_run: _runchange_AG( should_run )
   }
   
   rc = apply_instantion_and_runchange( gateway_directives, inst_funcs, runchange_funcs )
   if rc != 0:
      log.error("Failed to alter gateway status for volume %s, rc = %s" % (volume_name, rc) )
      
   return rc 


#-------------------------
def start_stop_all_volumes( config, volume_info_list, slice_secret, hostname=None, ignored=[], gateway_uid_name=None, gateway_gid_name=None, debug=False ):
   """
   Synchronize the states of all volumes on this host, stopping any volumes that are no longer attached.
   """
   
   success_volumes = []
   failed_volumes = []
   
   # methods that stop gateways, and take the volume name as their only argument
   stoppers = {
      "UG": ensure_UG_stopped, # NOTE: mountpoint can be ignored if we only care about the volume
      "RG": ensure_RG_stopped,
      "AG": ensure_AG_stopped
   }
   
   for volume_info in volume_info_list:
         
      volume_name = volume_info[ observer_cred.OPENCLOUD_VOLUME_NAME ]
      
      # get what we need...
      try:
         syndicate_url             = volume_info[ observer_cred.OPENCLOUD_SYNDICATE_URL ]
         principal_id              = volume_info[ observer_cred.OPENCLOUD_VOLUME_OWNER_ID ]
         principal_pkey_pem        = volume_info[ observer_cred.OPENCLOUD_PRINCIPAL_PKEY_PEM ]
         
      except:
         log.error("Invalid configuration for Volume %s" % volume_name)
         continue
      
      # connect to syndicate 
      client = syntool.Client( principal_id, syndicate_url, user_pkey_pem=principal_pkey_pem, debug=config['debug'] )

      log.info("Sync volume %s" % volume_name )
      
      rc = start_stop_volume( config, volume_info, slice_secret, client=client, hostname=hostname, gateway_uid_name=gateway_uid_name, gateway_gid_name=gateway_gid_name, debug=debug )
      
      if rc == 0:
         log.info("Successfully sync'ed %s" % volume_name )
         success_volumes.append( volume_name )
      
      else:
         log.error("Failed to sync volume %s, rc = %s" % (volume_name, rc))
         failed_volumes.append( volume_name )
   
   # find the running gateways 
   running_gateways = list_running_gateways_by_volume()
   
   for volume_name, gateway_info in running_gateways.items():
      
      # this volume isn't present, and we're not ignoring it?
      if volume_name not in success_volumes and volume_name not in failed_volumes and volume_name not in ignored:
         
         # volume isn't attached...killall of its gateways 
         for gateway_type in ["UG", "RG", "AG"]:
            
            rc = stoppers[gateway_type]( volume_name )
            
            if rc != 0:
               log.error("Failed to stop %s for %s, rc = %s" % (gateway_type, volume_name, rc))
            
            failed_volumes.append( volume_name )

   if len(failed_volumes) != 0:
      return -errno.EAGAIN 
   
   else:
      return 0
   

      