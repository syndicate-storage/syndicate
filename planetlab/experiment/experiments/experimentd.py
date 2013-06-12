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
import dateutil
import BaseHTTPServer
import SocketServer
import urlparse
import cgi
import httplib

PORT = 40001
HOSTNAME = socket.gethostname()
DATA_SERVER_URL = "http://vcoblitz-cmi.cs.princeton.edu/"
#DATA_SERVER_URL = "http://localhost:8000/"

LOG_FD = None
VERBOSE = True

LAST_CMD = "/tmp/last-command"

HEARTBEAT_URL = os.path.join( DATA_SERVER_URL, "heartbeat" )

EXPERIMENT_PROC = None

PIDFILE="/tmp/experimentd.pid"
LOGFILE="/tmp/experimentd.log"

# log a message
def log( loglevel, msg ):
   txt = "[%s] %s: %s" % (time.ctime(), loglevel, msg)

   if LOG_FD != None:
      print >> LOG_FD, txt
      LOG_FD.flush()

   if VERBOSE:
      print txt


# post a message to the experiment server
def post_message( wfile, loglevel, msg_txt ):
   global VERBOSE

   if VERBOSE:
      log( loglevel, msg_txt )

   wfile.write("[%s] %s: %s\n" % (time.ctime(), loglevel, msg_txt ))
   

# stop a subprocess
def stop_subprocess( wfile, proc, name ):
   if proc == None:
      return

   # try SIGINT, then SIGKILL
   try:
      rc = proc.poll()
      if rc != None:
         post_message( wfile, "INFO", "%s exited, rc = %s" % (name, rc))
         return
   except:
      post_message( wfile, "INFO", "%s presumed dead" % (name) )
      return


   # sigint it
   try:
      proc.send_signal( signal.SIGINT )
   except:
      post_message( wfile, "INFO", "%s signal failure; presumed dead" % (name) )
      return

   attempts = 5
   while attempts > 0:

      try:
         rc = proc.poll()
         if rc != None:
            break
      except:
         break

      time.sleep(1)
      attempts -= 1

   if rc == None:
      # client isn't dead yet
      try:
         proc.kill()
         post_message( wfile, "WARN", "%s had to be killed" % name )
         return
      except:
         post_message( wfile, "WARN", "kill %s failed; presumed dead" % (name) )
         return

   post_message( wfile, "INFO", "%s exit code %s" % (name, rc) )
   return


# stop an experiment
def stop_experiment( wfile, experiment_proc ):
   return stop_subprocess( wfile, experiment_proc, "experiment" )


# run an experiment
def run_experiment( wfile, experiment_url ):
   global EXPERIMENT_PROC

   # first, download the experiment
   experiment_name = os.path.basename(experiment_url)
   experiment_path = "/tmp/%s" % experiment_name
   experiment_fd = open(experiment_path, "w")

   rc = subprocess.call(["curl", "-ks", experiment_url], stdout=experiment_fd)
   experiment_fd.close()
   if rc != 0:
      post_message(wfile, "ERROR", "could not download experiment %s, rc = %s" % (experiment_url, rc))
      return rc

   try:
      os.chmod( experiment_path, 0755 )
   except:
      post_message(wfile, "ERROR", "could not chmod %s, rc = %s" % (experiment_path, rc))
      return None

   # run the experiment!
   experiment_data = None
   experiment_log = None
   try:
      experiment_log = open(experiment_path + ".log", "w")
      EXPERIMENT_PROC = subprocess.Popen( [experiment_path], stdout=experiment_log, stderr=experiment_log )
      rc = EXPERIMENT_PROC.wait()
      experiment_log.close()
   except Exception, e:
      experiment_data = str(e)
      try:
         if experiment_log != None:
            experiment_log.close()
      except:
         pass

   EXPERIMENT_PROC = None
   if experiment_data == None:
      try:
         experiment_log = open(experiment_path + ".log", "r")
         experiment_data = experiment_log.read()
         experiment_log.close()
      except:
         experiment_data = "NO DATA"

   post_message(wfile, "INFO", "\n---------- BEGIN %s ----------\n%s\n---------- END %s ----------\n" % (experiment_name, experiment_data, experiment_name) )
   return rc


# upgrade ourselves
def upgrade( wfile, upgrade_url ):

   global EXPERIMENT_PROC
   
   fd = open("/tmp/experimentd.py", "w")
   rc = subprocess.call(["curl", "-ks", upgrade_url], stdout=fd)
   fd.close()
   if rc != 0:
      return rc
   else:
      stop_experiment( wfile, EXPERIMENT_PROC )
      shutdown()
      os.chmod("/tmp/experimentd.py", 0755)
      os.execv("/tmp/experimentd.py", ["/tmp/experimentd.py", "-u"])   
   
# prepare for shutdown
def shutdown():
   global EXPERIMENT_PROC
   global LAST_CMD
   global PIDFILE

   stop_experiment( sys.stdout, EXPERIMENT_PROC )

   EXPERIMENT_PROC = None
   try:
      os.unlink( LAST_CMD )
   except:
      pass

   try:
      os.unlink( PIDFILE )
   except:
      pass

   LOG_FD.close()
   return



# signal handler
def cleanup( signal, frame ):
   shutdown()
   os._exit(0)


# process a command
def process_command( wfile, command ):
   global LAST_CMD
   global NEW_DAEMON_URL

   ok = True

   # format: COMMAND [args]
   command_name = command.split()[0]

   if command_name == "exit":
      post_message( wfile, "INFO", "shutting down" )
      cleanup( None, None )

   elif command_name == "upgrade":
      upgrade_url = command.split()[1]
      rc = upgrade( wfile, upgrade_url )
      if rc != 0:
         post_message( wfile, "ERROR", "upgrade rc = %s" % rc)

   elif command_name == "experiment":
      # verify that the client is running
      experiment_url = command.split()[1]
      rc = run_experiment( wfile, experiment_url )
      post_message( wfile, "INFO", "experiment %s rc = %s" % (os.path.basename(experiment_url), rc) )

   elif command_name == "nothing":
      ok = True

   else:
      ok = True

   return ok


# check the heartbeat status every so often
class HBThread( threading.Thread ):
   def run(self):
      global HEARTBEAT_URL
      global EXPERIMENT_PROC
      while True:
         req = urllib2.Request( HEARTBEAT_URL )
         dat = None
         try:
            resp = urllib2.urlopen( req )
            dat = resp.read()
            resp.close
         except Exception, e:
            log( "EXCEPTION", "Failed to read %s, exception = %s" % (HEARTBEAT_URL, e) )

         if dat != None:
            dat = dat.strip()
            if dat == "force-stop" and EXPERIMENT_PROC != None:
               # kill everything
               stop_experiment( sys.stdout, EXPERIMENT_PROC )
               EXPERIMENT_PROC = None
               post_message( sys.stdout, "INFO", "HBThread: stopped experiment" )
               
            elif dat == "exit":
               cleanup( None, None )
               os._exit(0)

         time.sleep(300)


# wait until we get a new command
class CommandListener( BaseHTTPServer.BaseHTTPRequestHandler ):
   def do_POST(self):
      # get the parameters
      content_len = int(self.headers.getheader("Content-Length"))
      encoded_data = self.rfile.read( content_len )
      data = cgi.parse_qs(encoded_data)

      self.send_response( 200 )
      self.end_headers()
      
      command = data['command'][0]
      process_command( self.wfile, command )
      return


   def do_QUIT(self):
      self.send_response(200)
      self.end_headers()
      os._exit(0)
      

class ExperimentServer( SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer ):

   def serve_forever(self):
       self.stop = False
       while not self.stop:
          self.handle_request()



if __name__ == "__main__":
   if os.path.exists(PIDFILE):
      pid = 0
      if '-u' in sys.argv:
         pidf = open(PIDFILE,"r")
         try:
            pid = int( pidf.read().strip() )
            os.kill( pid, signal.SIGTERM )
            time.sleep(1.0)
            os.kill( pid, signal.SIGKILL )
         except:
            pass

         os.unlink( PIDFILE )
      else:
         post_message( sys.stderr, "ERR", "experimentd already running" )
         sys.exit(1)

   mypid = os.getpid()
   fd = open(PIDFILE, "w" )
   fd.write( str(mypid) )
   fd.close()

   LOG_FD = open(LOGFILE, "w" )
   HOSTNAME = socket.gethostname()

   signal.signal( signal.SIGINT, cleanup )
   signal.signal( signal.SIGQUIT, cleanup )
   signal.signal( signal.SIGTERM, cleanup )
   signal.signal( signal.SIGPIPE, signal.SIG_IGN )
   
   # heartbeat
   hb_thread = HBThread()
   hb_thread.setDaemon(True)
   hb_thread.start()

   if '-u' in sys.argv:
      post_message( sys.stdout, "INFO", "upgraded" )

   httpd = ExperimentServer( ('', PORT), CommandListener )
   httpd.serve_forever()
   
   cleanup( None, None )
   
