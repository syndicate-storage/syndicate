#!/usr/bin/python

"""
Command and control daemon that runs on each node in the metadata service.
It is responsible for managing metadata servers on its node.
"""
import sys
import os
import subprocess
import socket
import time
import random
import signal
import traceback
import StringIO
import threading
import shutil

sys.path.append("/usr/share/SMDS")

from SMDS.xmlrpc_ssl import *
from SMDS.faults import *
from SMDS.config import Config
import SMDS.logger as logger

# global configuration
conf = None

# track available ports for MS volumes
ms_portset = None

# track volume IDs
vol_id_counter = None

# track UIDs
uid_counter = None

# determine the control root directory for a volume
def VOLUME_CTL_ROOT( c, mdserver_dict ):
   return os.path.join( c['MD_CTL_DIR'], os.path.join("volumes", str( mdserver_dict['NAME'] )) )

# determine configuration file name
def VOLUME_CONF_PATH( ctl_root ):
   return os.path.join( ctl_root, "md_server.conf" )

# determine logfile name
def LOGFILE_PATH( ctl_root ):
   return os.path.join( ctl_root, "md_logfile.txt" )

# determine pid file name
def PIDFILE_PATH( ctl_root ):
   return os.path.join( ctl_root, "md_pid" )

# determine secrets file name
def SECRETS_PATH( ctl_root ):
   return os.path.join( ctl_root, "md_secrets.txt" )

# get the names of all existing volumes
def VOLUME_NAMES( c ):
   return os.listdir( os.path.join( c['MD_CTL_DIR'], "volumes" ) )

   
class PortSet:
   """
   Set of ports for allocating to MS processes
   """
   
   def __init__( self, low, high ):
      self.low = low
      self.high = high
      self.allotted_ports = []
      self.allotted_ports_lock = threading.Lock()

      
   def load( self, c ):
      """
      Walk through the MS config root and read the list of ports in each config.
      """
      volume_names = VOLUME_NAMES( c )
      for volume in volume_names:
         # get the control root directory for this volume
         ctl_root = VOLUME_CTL_ROOT( c, {'NAME': volume} )

         # get the config for this volume
         vol_conf_path = VOLUME_CONF_PATH( ctl_root )

         # read it
         vol_conf = None 
         try:
            vol_conf = read_config( vol_conf_path, ['PORTNUM'] )
            if vol_conf:
               self.register_ports( [ int(vol_conf['PORTNUM']) ] )
         except:
            pass
      

   def register_ports( self, portlist ):
      """
      Add a set of ports to this PortSet.
      Return True if all added; False if any overlap.
      """
      
      ret = True
      self.allotted_ports_lock.acquire()
      
      for p in portlist:
         if p in self.allotted_ports:
            ret = False
            break

      if ret:
         self.allotted_ports += portlist
         
      self.allotted_ports_lock.release()
      return ret
      

   def unregister_ports( self, portlist ):
      """
      Remove a set of ports from this PortSet.
      """
      self.allotted_ports_lock.acquire()

      for p in portlist:
         try:
            self.allotted_ports.remove( p )
         except:
            pass

      self.allotted_ports_lock.release()


   def unavailable_ports( self ):
      return self.allotted_ports[:]


class AtomicCounter:
   """
   Atomic counter implementation
   """
   def __init__(self, start=0, save=None):
      self.value = start
      self.lock = threading.Lock()
      self.fd = None
      if save != None:
         # read old value from disk
         if os.path.isfile( save ):
            self.fd = open(save, "r+")
            data = self.fd.read()

            try:
               self.value = int(data)
            except Exception, e:
               self.fd.close()
               self.fd = open(save, "w")
         else:
            self.fd = open(save, "w")
            

   def save_nolock( self ):
      if self.fd != None:
         self.fd.seek(0)
         self.fd.write( str(self.value) )
         self.fd.flush()
         
   def save( self ):
      if self.fd != None:
         self.lock.acquire()
         self.save_nolock()
         self.lock.release()
         
   def add( self, c ):
      self.lock.acquire()
      self.value += c
      self.save_nolock()
      self.lock.release()

   def next( self ):
      self.lock.acquire()
      ret = self.value
      self.value += 1
      self.save_nolock()
      self.lock.release()
      return ret

   def get( self ):
      return self.value

   

# make sure the required attributes are present
def conf_required( c, required_attrs ):
   for attr in required_attrs:
      if not c.has_key( attr ):
         return False

   return True

# make sure only the given attributes are present
def conf_forbidden( c, allowed_attrs ):
   for attr in c.keys():
      if attr not in allowed_attrs:
         return True

   return False
   
   
def read_config( config_file, required_attrs=None ):
   """
   Read the configuration, and assert that every required attribute
   is present.
   """

   try:
      fd = open( config_file, "r" )
   except Exception, e:
      logger.exception( e, "Could not open %s" % config_file )
      return None


   c = {}
   valid = True
   
   while True:
      line = fd.readline()
      if len(line) == 0:
         break
         
      line = line.strip()
      
      if len(line) == 0:
         continue

      if line[0] == '#':
         continue

      parts = line.split('=')
      if len(parts) < 2:
         logger.error( "Invalid config line %s" % config_file )
         valid = False

      if valid:
         varname = parts[0].strip()
         
         # collate values
         values = []
         for value in parts[1:]:
            s = value.strip('"')
            values.append( s )

         if len(values) == 1:
            values = values[0]      # this is a scalar

         # keep list of values if variable occurs multiple times
         if c.has_key( varname ):
            if type( c[varname] ) != type([]):
               tmp = c[varname]
               c[varname] = [tmp]

            else:
               c[varname].append( values )

         else:
            c[varname] = values

   fd.close()
   if not valid:
      return None
      
   attr = None
   try:
      if required_attrs:
         for rc in required_attrs:
            attr = rc
            assert rc in c
   except AssertionError, ae:
      raise MDInternalError( "read_config assert %s failed" % attr )

   return c
      

def make_secret_entry( user_dict ):
   """
   Given a user, return a line of text suitable for a secrets file entry.
   """
   try:
      assert 'uid' in user_dict
      assert 'username' in user_dict
      assert 'password' in user_dict
   except:
      raise MDInternalError( "make_secret_entry assertion failed" )
   
   return "%s:%s:%s" % (user_dict['uid'], user_dict['username'], user_dict['password'])
   

def cleanup_ctl_dir( ctl_dir ):
   """
   Remove all entries in a control directory
   """
   try:
      os.system("/bin/rm -rf %s" % ctl_dir )
   except:
      pass
   

def generate_config_file( defaults_file, template_file, extra_config=None ):
   """
   Generate a configuration file from a template file, default values file, and extra configuration data
   """
   
   # read the default configuration file and add in the extras
   conf_vars = {}

   try:
      conf_vars = read_config( defaults_file )
   except:
      pass
   
   if extra_config and isinstance( extra_config, dict):
      conf_vars.update( extra_config )

   stdout = StringIO.StringIO()
   stderr = StringIO.StringIO()
   
   # rendering function
   def OUT( s ):
      stdout.write( s )
      stdout.write("\n")

   def ERR( s ):
      stderr.write( s )
      stderr.write("\n")

   # evaluate our config-generating script
   try:
      template_fd = open(template_file, "r")
      template_code = template_fd.read()
      template_fd.close()

      conf_vars['OUT'] = OUT
      conf_vars['ERR'] = ERR
      
      exec template_code in conf_vars

      config_data = stdout.getvalue()
      config_err = stderr.getvalue()
      
   except Exception, e:
      raise MDMethodFailed( "generate_config_file", e )

   return (config_data, config_err)

   
   
def write_config_file( output_file, data ):
   """
   Write out a config file
   """
   
   try:
      f = open( output_file, "w" )
      f.write( data )
      f.close()
   except Exception, e:
      raise MDMethodFailed( "write_config_file", e )
   
   return



def read_secrets_file( secrets_file ):
   """
   Read a secrets file and return a list of users
   """
   try:
      sf = open( secrets_file, "r" )
   except Exception, e:
      raise MDMethodFailed( "read_secrets_file", e )

   users = []
   lineno = 0
   while True:
      line = sf.readline()
      lineno += 1
      
      if len(line) == 0:
         break

      line = line.strip()
      if len(line) == 0:
         continue

      parts = line.split(":")
      if len(parts) != 3:
         logger.warn("Invalid line %s in %s" % (lineno, secret_line))
         continue
         
      users.append( user_entry( int(parts[0]), parts[1], parts[2] ) )

   return users

   
   
def write_secrets_file( user_list, secrets_file ):
   """
   Generate a secrets file from a list of user dictionaries.
   """
   
   # create the secrets file
   try:
      sf = open( secrets_file, "w" )
   except Exception, e:
      raise MDMethodFailed( "write_secrets_file", e )
   
   for user in user_list:
      secret_line = make_secret_entry( user )
      try:
         sf.write( secret_line + "\n" )
      except Exception, e:
         raise MDMethodFailed( "write_secrets_file", e )
   
   sf.close()
   
   return
      

def user_entry( uid, username, password_hash ):
   return {'uid': uid, 'username': username, 'password': password_hash}

def va_entry( username, password_hash ):
   return {'uid': 0, 'username': username, 'password': password_hash}
   
   
def get_open_ports( ctl_dir, port_low, port_high, num_ports ):
   """
   Get a list of available port numbers, using netstat
   """

   global ms_portset
   
   # get a list of open ports
   try:
      open_ports_sh = "netstat -tuan --numeric-hosts | tail -n +3 | awk '{n=split($4,a,\":\"); print a[n]}'"
      proc = subprocess.Popen( open_ports_sh, stdout=subprocess.PIPE, shell=True )
      ports_str, _ = proc.communicate()
      proc.wait()
      
   except Exception, e:
      raise MDMethodFailed( "get_open_ports", e )


   used_ports = [int(x) for x in ports_str.split()]
   unavailable_ports = [port_low - 1] + filter( lambda x: x >= port_low and x < port_high, used_ports ) + ms_portset.unavailable_ports() + [port_high + 1]
   unavailable_ports.sort()

   if port_high - port_low - len(unavailable_ports) < num_ports:
      raise MDInternalError( "get_open_ports: not enough open ports" )

   ret = []
   for i in xrange(0, num_ports):
      c = random.randint( 0, port_high - port_low - (len(unavailable_ports) - 2) )
      p = 0
      h = 0
      s = 0

      for k in xrange(0,len(unavailable_ports)-1):
         h = unavailable_ports[k]
         ran = unavailable_ports[k+1] - unavailable_ports[k] - 1
         if p + ran < c:
            p += ran
            continue
         else:
            s = h + 1 + c - p
            break

      ret.append( s )
      unavailable_ports.append( s )
      unavailable_ports.sort()

   rc = ms_portset.register_ports( ret )
   if not rc:
      raise MDInternalError( "get_open_ports: not enough open ports" )
   
   return ret

   

def install_volume_config( mdserver_dict, users_list, extra_params ):
   """
   Install the configuration files for a volume.
   """
   
   global conf

   ctl_root = VOLUME_CTL_ROOT( conf, mdserver_dict )
   
   # create the secrets file, if we have secrets data
   if users_list != None:
      secrets_file = SECRETS_PATH( ctl_root )
      try:
         write_secrets_file( users_list, secrets_file )
         extra_params['SECRETS_FILE'] = secrets_file
      except Exception, e:
         raise e

   # create a config file
   if mdserver_dict != None or extra_params != None:
      config_file = VOLUME_CONF_PATH( ctl_root )
      try:
         all_config = {}

         if mdserver_dict != None:
            all_config.update( mdserver_dict )

         if extra_params != None:
            all_config.update( extra_params )

         out, err = generate_config_file( conf['MD_CONFIG_DEFAULTS'], conf['MD_CONFIG_TEMPLATE'], all_config )

         write_config_file( config_file, out )
      except Exception, e:
         raise e

   return 0




def get_volume_pid( md_pidfile ):
   """
   Get the PID form a pidfile
   """
   try:
      pid_f = open(md_pidfile, "r")
   except:
      return None

   try:
      pid = int( pid_f.read() )
      pid_f.close()

      return pid
   except:
      raise MDInternalError( "corrupt PID file" )
      

      
def is_volume_running( md_pidfile ):
   """
   Determine whether or not a metadata server with the given PID file is running
   """
   global conf

   pid = get_volume_pid( md_pidfile )
   if pid == None:
      return False

   try:
      os.kill( pid, 0 )
      return True
   except:
      # no such PID
      return False


def get_io_urls( md_conf ):
   """
   Generate the read/write URLs for a volume
   """
   
   proto = 'http://'
   if md_conf.has_key( 'SSL_PKEY' ) and md_conf.has_key( 'SSL_CERT' ):
      proto = "https://"

   host = socket.gethostname()
   
   read_url = proto + host + ":" + str(md_conf['PORTNUM']) + "/"
   write_url = None

   if md_conf['AUTH_OPERATIONS'] == "readwrite" or md_conf['AUTH_OPERATIONS'] == "write":
      write_url = proto + host + ":" + str(md_conf['PORTNUM']) + "/"

   return (read_url, write_url)


def reload_volume( mdserver_name ):
   """
   Reload a volume's configuration--send it a message to reload.
   Raise an exception if the volume isn't running or can't be reached
   """
   global conf
   
   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   pidfile_path = PIDFILE_PATH( ctl_root )

   # extract the pid 
   pid = get_volume_pid( pidfile_path )
   if pid == None:
      raise MDMethodFailed( "reload_volume", "Could not get volume PID")

   # reload
   print "reload_volume: about to reload %s (pid = %s)" % (mdserver_name, pid)
   os.system("ps aux | grep mdserverd")

   print "command: %s -k %s" % (conf['MD_BINARY'], str(pid))
   md_proc = subprocess.Popen( [conf['MD_BINARY'], '-k', str(pid)], close_fds = True )
   rc = md_proc.wait()

   time.sleep(1.0)
   print "reload_volume: reloaded, rc = %s" % rc
   os.system("ps aux | grep mdserverd")
      

   return rc
   

def create_volume( mdserver_name, mdserver_dict, va_username, va_pwhash ):
   """
   Given a dictionary containing the fields of a metadata server
   and a list of dictionaries describing each user to run this server,
   set up a metadata server (but don't start it)
   """

   global conf
   global vol_id_counter
   
   required_attrs = [
      'AUTH_OPERATIONS',
      'BLOCKING_FACTOR',
   ]

   # for now, fill in defaults
   if 'AUTH_OPERATIONS' not in mdserver_dict.keys():
      mdserver_dict['AUTH_OPERATIONS'] = "readwrite"

   if 'BLOCKING_FACTOR' not in mdserver_dict.keys():
      mdserver_dict['BLOCKING_FACTOR'] = 102400
      
   
   if not conf_required( mdserver_dict, required_attrs ):
      raise MDInvalidArgument( "Missing attributes. Required: %s" % (', '.join(required_attrs)), 'create_volume' )
   
   # create the directory to store information on 
   ctl_root = VOLUME_CTL_ROOT( conf, mdserver_dict )
   if os.path.isdir( ctl_root ):
      raise MDMethodFailed( "create_volume", "Volume '%s' already exists" % mdserver_name )
   
   cleanup_ctl_dir( ctl_root )
   
   try:
      os.makedirs( ctl_root )
   except Exception, e:
      cleanup_ctl_dir( ctl_root )
      raise MDMethodFailed( "create_volume setup", e )
   
   
   # create the master copy
   mc_root = os.path.join( ctl_root, "master_copy" )
   
   try:
      os.makedirs( mc_root )
   except Exception, e:
      cleanup_ctl_dir( ctl_root )
      raise MDMethodFailed( "create_volume mcroot", e )
   
   # create the PID file path (for the config)
   md_pidfile = PIDFILE_PATH( ctl_root )
   
   # get an HTTP and query port
   md_portnums = get_open_ports( conf['MD_CTL_DIR'], int(conf['MD_CTL_PORT_LOW']), int(conf['MD_CTL_PORT_HIGH']), 2 )
   http_portnum = md_portnums[0]
   query_portnum = md_portnums[1]
   volume_id = vol_id_counter.next()
   
   try:

      # install this volume's configuration
      params = {
            'MDROOT':         mc_root,
            'PORTNUM':        str(http_portnum),
            'QUERY_PORTNUM':  str(query_portnum),
            'PIDFILE':        md_pidfile,
            'AUTH_OPERATIONS':mdserver_dict['AUTH_OPERATIONS'],
            'BLOCKING_FACTOR':mdserver_dict['BLOCKING_FACTOR'],
            'SSL_PKEY':       conf['MD_SSL_PKEY'],
            'SSL_CERT':       conf['MD_SSL_CERT'],
            'VOLUME_ID':      volume_id
      }

      read_url, write_url = get_io_urls( params )

      if read_url:
         params[ 'METADATA_READ_URL' ] = read_url
      if write_url:
         params[ 'METADATA_WRITE_URL' ] = write_url
      
      user_list = [va_entry( va_username, va_pwhash )]
      install_volume_config( mdserver_dict, user_list, params )
      
   except Exception, e:
      cleanup_ctl_dir( ctl_root )
      raise e
   
   return 1



# lock to prevent concurrent create_VACE operations
VACE_lock = threading.Lock()

def create_VACE( mdserver_name, username, pwhash, role ):
   """
   Associate a given user with a role in the context of a given volume.  Return 1 on success.
   """
   global conf
   global uid_counter

   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   secrets_file = SECRETS_PATH( ctl_root )

   VACE_lock.acquire()

   users = []
   try:
      # is this user represented?
      users = read_secrets_file( secrets_file )
   except Exception, e:
      VACE_lock.release()
      raise e

      
   for user in users:
      if user['username'] == username:
         VACE_lock.release()
         raise MDInvalidArgument( "User '%s' already exists\n" % username )
         

   new_user = None
   if role == "VA" or role == "SA":
      # volume or syndicate admin--they get all rights
      new_user = va_entry( username, pwhash )
   else:
      uid = uid_counter.next()
      new_user = user_entry( uid, username, pwhash )

   users.append( new_user )

   try:
      write_secrets_file( users, secrets_file )
   except Exception, e:
      VACE_lock.release()
      raise e

   VACE_lock.release()

   if is_volume_running( PIDFILE_PATH( ctl_root ) ):
      rc = reload_volume( mdserver_name )
      if rc != 0:
         raise MDInternalError( "Failed to reload '%s'\n", mdserver_name )

   return 1


def delete_VACE( mdserver_name, username_or_id ):
   """
   Delete the user from a volume.  Return 1 on success.
   """
   global conf

   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   secrets_file = SECRETS_PATH( ctl_root )

   VACE_lock.acquire()
   try:
      users = read_secrets_file( secrets_file )
   except Exception, e:
      VACE_lock.release()
      raise e

   found_idx = -1
   for i in xrange(0,len(users)):
      user = users[i]
      if user['username'] == username_or_id or user['uid'] == username_or_id:
         found_idx = i
         break

   if found_idx == -1:
      # not found
      VACE_lock.release()
      raise MDInvalidArgument( "No such user '%s'" % username_or_id )

   users.remove( users[found_idx] )

   try:
      write_secrets_file( users, secrets_file )
   except Exception, e:
      VACE_lock.release()
      raise e

   VACE_lock.release()

   if is_volume_running( PIDFILE_PATH( ctl_root ) ):
      rc = reload_volume( mdserver_name )
      if rc != 0:
         raise MDInternalError( "Failed to reload '%s'\n", mdserver_name )

   return 1


   
def start_volume( mdserver_name ):
   """
   Start up an existing metadata server for a volume.  Return 1 on success.
   """
   
   global conf
   
   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   config_file = VOLUME_CONF_PATH( ctl_root )
   md_logfile = LOGFILE_PATH( ctl_root )
   md_pidfile = PIDFILE_PATH( ctl_root )
   
   # Get this server's configuration file
   try:
      md_conf = read_config( config_file )
   except Exception, e:
      raise MDMethodFailed( 'start_volume', "read config exception = '%s'" % e )
   
   # make sure we're not running...
   if is_volume_running( md_pidfile ):
      return 1

   try:
      assert os.path.isdir( ctl_root ), "Control directory does not exist"
      assert os.path.isfile( config_file ), "Config file does not exist" 
      assert os.path.isdir( md_conf['MDROOT'] ), "Master copy '%s' does not exist" % (md_conf['MDROOT'])
   except AssertionError, e:
      raise MDInternalError( "Server is not fully set up: %s" % str(e) )
   
   try:
      assert not os.path.isfile( md_pidfile )
   except AssertionError, e:
      raise MDInternalError( "Server is already running" )
   
   # fire up the binary
   md_proc = subprocess.Popen( [conf['MD_BINARY'], '-c', config_file, '-l', md_logfile ], close_fds = True )
   rc = md_proc.wait()
   
   if rc != 0:
      # could not start the server
      # make sure we remove runtime files, just in case

      try:
         os.unlink( md_pidfile )
      except:
         pass
      
      raise MDMethodFailed( "start_volume", "rc = %s when starting metadata server" % rc )
   
   return 1
   

   
def stop_volume( mdserver_name ):
   """
   Stop a running metadata server.
   Return 1 on success
   Return 0 if the server isn't running
   """
   
   global conf
   
   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   md_pidfile = PIDFILE_PATH( ctl_root )
   md_logfile = LOGFILE_PATH( ctl_root )
   
   # if we're not running, then do nothing
   if not is_volume_running( md_pidfile ):
      # make sure these files don't exist
      
      try:
         os.unlink( md_pidfile )
      except:
         pass
      
      return 0
   
   try:
      assert os.path.isdir( ctl_root ), "Control directory does not exist"
   except AssertionError, e:
      raise MDInternalError( "Server is not correctly set up: %s" % str(e) )
   
   pid = get_volume_pid( md_pidfile )
   if pid == None:
      raise MDMethodFailed( "stop_volume", "could not read PID file: %s" + str(e))
      
   else:
      # send SIGTERM to this metadata server
      os.kill( pid, signal.SIGTERM )
   
      dead = False
      
      # wait until the process dies...
      t_start = time.time()
      while time.time() - t_start < 10:
         try:
            os.kill( pid, 0 )
            time.sleep(0.1)
         except:
            dead = True
            break    # process is dead
      
      if not dead:
         # the process still runs, or it crashed
         # kill -9 either way.
         os.kill( pid, signal.SIGKILL )
   
   try:
      os.unlink( md_pidfile )
   except:
      pass
   
   try:
      os.rename( md_logfile, md_logfile + "." + str(int(time.time())) )
   except:
      pass
   
   return 1
   
   
def delete_volume( mdserver_name ):
   """
   Destroy a volume
   """
   global conf
   global ms_portset
   
   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   
   try:
      stop_volume( mdserver_name )
   except:
      pass

   # release ports
   try:
      config = read_config( VOLUME_CONF_PATH( ctl_root ), ['PORTNUM', 'QUERY_PORTNUM'] )
      ms_portset.unregister_ports( [config['PORTNUM'], config['QUERY_PORTNUM']] )
   except:
      pass
   
   cleanup_ctl_dir( ctl_root )
   
   return 1


def read_volume( mdserver_name, fields ):
   """
   Read configuration fields from a volume, given its name
   """
   global conf

   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   conf_path = VOLUME_CONF_PATH( ctl_root )

   try:
      vol_conf = read_config( conf_path, fields )
   except Exception, e:
      raise MDMethodFailed( "read_volume", "could not read config, exception = '%s'" % e )

   ret = {}
   for f in fields:
      ret[f] = vol_conf[f]
      
   return ret
      
   
def list_volumes( fields ):
   """
   Get a list of all volumes' fields
   """
   global conf

   volume_names = VOLUME_NAMES( conf )
   ret = []
   
   for name in volume_names:
      vol_conf = read_volume( name, fields )
      vol_conf['NAME'] = name
      ret.append( vol_conf )

   return ret
   
   
def update_volume( mdserver_name, mdserver_dict ):
   """
   Update this volume's configuration
   """

   global conf

   allowed_attrs = [
      'REPLICA_URL',
      'AUTH_OPERATIONS',
      'NAME'
   ]

   if conf_forbidden( mdserver_dict, allowed_attrs ):
      raise MDInvalidArgument( "Invalid attributes.  Allowed: %s" % (', '.join(allowed_attrs)), "update_volume" )
   
   ctl_root = VOLUME_CTL_ROOT( conf, {'NAME': mdserver_name} )
   conf_file = VOLUME_CONF_PATH( ctl_root )
   try:
      assert os.path.isdir( ctl_root ), "Control directory does not exist"
      assert os.path.isfile( conf_file ), "Control directory does not have a server config"
   except AssertionError, e:
      raise MDInvalidArgument( "Cannot use new name %s: path exists" % new_name )
   
   restart = True               # restart after we regenerate the secrets and config
   try:

      # make a config that uses the old name of this volume
      rc = stop_volume( mdserver_name )
      if rc == 0:
         # wasn't running in the first place
         restart = False
         
   except Exception, e:
      raise MDMethodFailed( "Could not stop server", e )
   
   # regenerate the config and secrets
   # get the old config
   old_conf = {}
   try:
      old_conf = read_config( conf_file )
   except Exception, e:
      raise MDMethodFailed( "Could not read old config", e )

   # migrate data to new name, if need be
   if mdserver_dict.has_key( 'NAME' ) and mdserver_name != mdserver_dict['NAME']:
      new_ctl_root = VOLUME_CTL_ROOT( conf, mdserver_dict )
      shutil.move( ctl_root, new_ctl_root )
      ctl_root = new_ctl_root

   mdserver_dict['NAME'] = mdserver_name
      
   # regenerate the config
   try:
      install_volume_config( mdserver_dict, None, old_conf )
   except Exception, e:
      raise MDMethodFailed( "update_volume", "Could not generate config file, exception = %s" % e )
      
   except Exception, e:
      cleanup_ctl_dir( ctl_root )
      raise e
   
   # restart the server, if it was running before
   if restart:
      try:
         start_volume( mdserver_name )
      except Exception, e:
         raise MDMethodFailed( "Could not start server", e )
   
   return 1
   
   
   
def init():
   global conf
   global ms_portset
   global vol_id_counter
   global uid_counter

   logger.init()
   conf = read_config( "/etc/syndicate/syndicate-metadata-service.conf" )
   ctl_root = conf['MD_CTL_DIR']
   
   if not os.path.isdir( os.path.join(ctl_root, "volumes") ):
      os.makedirs( os.path.join(ctl_root, "volumes") )

   ms_portset = PortSet( conf['MD_CTL_PORT_LOW'], conf['MD_CTL_PORT_HIGH'] )
   ms_portset.load( conf )
   
   vol_id_counter = AtomicCounter( 1, os.path.join( ctl_root, "volid" ) )
   uid_counter = AtomicCounter( 10000, os.path.join( ctl_root, "uid" ) )


def test():
   mdserver_dict = {
      'NAME': "test",
      'AUTH_OPERATIONS': 'readwrite',
      'BLOCKING_FACTOR': 61440
   }

   print "create_volume"
   rc = create_volume( "test", mdserver_dict, "jcnelson", "0123456789abcdef" )
   print "create_volume rc = %s" % rc

   print ""
   
   print "create_VACE"
   rc = create_VACE( "test", "llp", "abcdef0123456789", "VP" )
   print "create_VACE rc = %s" % rc

   print ""

   print "read_volume"
   fields = ["METADATA_READ_URL", "METADATA_WRITE_URL"]
   rc = read_volume( "test", fields )
   print "read_volume(%s) rc = %s" % (','.join(fields), rc)

   print ""
   
   print "start_volume"
   rc = start_volume( "test" )
   print "start_volume rc = %s" % rc

   print ""

   os.system("ps aux | grep mdserverd; ls /tmp/syndicate_md/volumes/test/")

   print "create_VACE"
   rc = create_VACE( "test", "foo", "asdfasdfasdf", "VP" )
   print "create_VACE rc = %s" % rc

   time.sleep(1.0)
   os.system("ps aux | grep mdserverd; ls /tmp/syndicate_md/volumes/test/")
   
   print "update_volume"
   rc = update_volume( "test", {"REPLICA_URL": ["http://www.foo.com:12345/", "http://www.bar.com:23456/"]} )
   print "update_volume rc = %s" % rc

   print ""

   time.sleep(1.0)
   os.system("ps aux | grep mdserverd; ls /tmp/syndicate_md/volumes/test/")

   print "delete_VACE"
   rc = delete_VACE( "test", "foo" )
   print "delete_VACE rc = %s" % rc

   print ""

   time.sleep(1.0)
   os.system("ps aux | grep mdserverd; ls /tmp/syndicate_md/volumes/test/")


   #print "delete_VACE"
   #rc = delete_VACE( "test", "llp" )
   #print "delete_VACE(llp) rc = %s" % rc

   print ""

   #print "stop_volume"
   #rc = stop_volume( "test" )
   #print "stop_volume rc = %s" % rc
   
   
      
   
if __name__ == "__main__":
   # start up a server...
   init()
   
   logfile = open(conf['MD_LOGFILE'], "a")

   #test()
   #sys.exit(0)
   
   server = MD_XMLRPC_SSL_Server( ("", int(conf['MD_CTL_RPC_PORT'])), MD_XMLRPC_RequestHandler, conf['MD_SSL_PKEY'], conf['MD_SSL_CERT'], logfile, True, cacerts=conf['MD_SSL_CACERT'], client_cert=True )
   server.register_multicall_functions()
   server.register_function( create_volume )
   server.register_function( delete_volume )
   server.register_function( read_volume )
   server.register_function( list_volumes )
   server.register_function( update_volume )
   server.register_function( start_volume )
   server.register_function( stop_volume )
   
   try:
      server.serve_forever()
   except Exception, e:
      logfile.flush()
      logfile.write( str(e) + "\n" + traceback.format_exc() + "\n" )
      logfile.close()
      
