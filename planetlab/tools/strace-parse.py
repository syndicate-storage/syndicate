#!/usr/bin/python

import os
import sys
import time
import stat
import traceback

# TODO: interleaving between open(), read(), write(), and close()?

fd_map = {}    # map (pid, file descriptor)s to FileData structures
path_map = {}  # map file paths to FileData structures

unfinished = {}  # map PID to unfinished syscall string, for parsing
cwd = {}         # map PID to current working directory

class SysCall:
   name = ''
   args = []
   retval = None
   pid = 0
   timestamp = 0

   def __init__( self, pid, timestamp, name, args, retval ):
      self.pid = pid
      self.name = name
      self.args = args
      self.retval = retval
      self.timestamp = timestamp


   def __repr__(self):
      return str(self.pid) + " " + str(self.timestamp) + " " + self.name + "( " + ", ".join(self.args) + ' ) --> ' + str(self.retval)
      

class FileData:

   reads = []      # list of (timestamp, start, end) byteranges
   writes = []     # list of (timestamp, start, end) byteranges
   opens = []      # list of (timestamp) open times
   closes = []     # list of (timestamp) close times 
   fds = []        # list of file descriptors
   fs_path = ""    # opened path
   offset = 0L     # offset of next I/O operation

   def __init__(self, path ):
      self.fs_path = path
      self.reads = []
      self.writes = []
      self.offset = 0L



def pidfd( pid, fd ):
   return str(pid) + "-" + str(fd)

# merge an unfinished...resume line
def merge_resumed_syscall( l1, l2 ):
   old_l1 = l1
   old_l2 = l2
   
   l1 = l1.replace("<unfinished ...>", "")
   syscall_name = l1.split()[2].split( '(' )[0]
   l2 = l2.replace("<... " + syscall_name + " resumed> ", "")
   
   return l1 + l2
   

# parse a line of strace data
def parse_strace_line( line ):
   global unfinished
   
   # format: PID call(arg1, arg2, ...)     = returncode [error name] [error string]
   parts = line.split()

   pid = int( parts[0] )
   timestamp = parts[1].strip()

   # outstanding unfinished syscall?
   if unfinished.has_key( pid ) and line.find("<...") >= 0 and line.find("resumed>") >= 0:
      unfinished_line = unfinished[ pid ]
      full_line = merge_resumed_syscall( unfinished_line, ' '.join(parts[2:]) )
      del unfinished[pid]
      return parse_strace_line( full_line )

   if line.find("<unfinished ...>") >= 0:
      unfinished[pid] = line
      return None
      
   start_call = 2
   end_call = 0
   for i in xrange(start_call, len(parts)):
      if parts[i].endswith(')'):
         end_call = i
         break


   if end_call < start_call:
      return None    # invalid input

   syscall_parts = parts[start_call].split('(')
   syscall_name = syscall_parts[0]
   args = [syscall_parts[1].strip(',)')]

   for i in xrange(start_call+1, end_call):
      args.append( parts[i].strip(',)') )

   # find the '='
   retval_idx = 0
   for i in xrange(end_call, len(parts)):
      if parts[i] == '=':
         retval_idx = i + 1
         break


   if retval_idx >= len(parts) or retval_idx == 0:
      return None    # invalid input

      
   retval = int(parts[retval_idx])

   return SysCall( pid, timestamp, syscall_name, args, retval )


# start logging information about a file
def open_file( syscall ):
   global fd_map
   global path_map
   global cwd
   
   if syscall.name != 'open' and syscall.name != 'creat':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   fd = syscall.retval
   path = syscall.args[0]

   if fd_map.has_key( pidfd(syscall.pid, fd) ):
      # problem--shouldn't be here
      print >> sys.stderr, "open_file: Duplicate fd %s" % fd
      return None

   if fd == -1:
      # failed to open
      print >> sys.stderr, "open_file: failed creat/open: %s" % syscall
      return None
      
   rec = None

   fp = path
   if cwd.has_key( syscall.pid ) and fp[0] != '/':
      fp = cwd[syscall.pid][:-1] + '/' + path[1:]
      
   if path_map.has_key( fp ):
      rec = path_map[ fp ]
   else:
      rec = FileData( fp )
      path_map[fp] = rec

   rec.fds.append( fd )
   rec.opens.append( syscall.timestamp )
   fd_map[ pidfd(syscall.pid, fd) ] = rec
   
   return True


   
# stop logging information about a file, since it got closed
def close_file( syscall ):
   global fd_map

   if syscall.name != 'close':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   fd = int(syscall.args[0])

   if fd_map.has_key( pidfd(syscall.pid, fd) ) and syscall.retval == 0:
      fd_map[ pidfd(syscall.pid, fd) ].closes.append( syscall.timestamp )
      fd_map[ pidfd(syscall.pid, fd) ].fds.remove( fd )
      del fd_map[ pidfd(syscall.pid, fd) ]
   elif syscall.retval != 0:
      print >> sys.stderr, "close_file: failed close: %s" % syscall
   else:
      return None

   return True



# log a read on a file
def read_file( syscall ):
   global fd_map

   if syscall.name != 'read':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   fd = int(syscall.args[0])
   read = syscall.retval

   if fd_map.has_key( pidfd(syscall.pid, fd) ):
      if read >= 0:
         rec = fd_map[ pidfd(syscall.pid, fd) ]
         rec.reads.append( (syscall.timestamp, rec.offset, rec.offset + read) )
         rec.offset = rec.offset + read
         return True
         
      else:
         print >> sys.stderr, "read_file: failed read: %s" % syscall
         return None

   else:
      print >> sys.stderr, "read_file: read on unknown fd %s" % fd
      return None



# log a write on a file
def write_file( syscall ):
   global fd_map

   if syscall.name != 'write':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   fd = int(syscall.args[0])
   write = syscall.retval

   if fd_map.has_key( pidfd(syscall.pid, fd) ):
      if write >= 0:
         rec = fd_map[ pidfd(syscall.pid, fd) ]
         rec.writes.append( (syscall.timestamp, rec.offset, rec.offset + write) )
         rec.offset = rec.offset + write
         return True

      else:
         print >> sys.stderr, "write_file: failed write: %s" % syscall
         return None

   else:
      print >> sys.stderr, "write_file: write on unknown fd %s" % fd
      return None


# log an lseek on the file
def lseek_file( syscall ):
   global fd_map

   if syscall.name != 'lseek':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   fd = int(syscall.args[0])
   off = syscall.retval

   if fd_map.has_key( pidfd(syscall.pid, fd) ):
      if off >= 0:
         rec = fd_map[ pidfd(syscall.pid, fd) ]
         rec.offset = off
         return True
      else:
         print >> sys.stderr, "lseek_file: failed lseek: %s" % syscall
         return None


   else:
      print >> sys.stderr, "lseek_file: lseek on unknown fd %s" % fd
      return None


# log a chdir
def chdir( syscall ):
   global cwd

   if syscall.name != 'chdir':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   cwd[syscall.pid] = syscall.args[0]
   return True


# log a fork
def fork( syscall ):
   global cwd

   if syscall.name != 'fork':
      print >> sys.stderr, "Invalid syscall %s" % syscall
      return None

   if syscall.retval > 0:
      # preserve data across the fork()
      cwd[ syscall.retval ] = cwd.get( syscall.pid )

   return True


# print the I/O operations by file
def print_by_file():
   global path_map
   
   # do some number crunching
   filenames = list(path_map.keys())
   filenames.sort()
   for filename in filenames:

      print filename.strip('"')

      print "size"
      try:
         print os.stat( filename.strip('"') ).st_size
      except:
         print "UNKNOWN"
      print "\n"

      print "reads"
      for (timestamp, read_start, read_stop) in path_map[filename].reads:
         print "%s-%s" % (read_start, read_stop)

      print "\nwrites"
      for (timestamp, write_start, write_end) in path_map[filename].writes:
         print "%s-%s" % (write_start, write_end)

      print "\n"



def get_next_timestamp( rec, cur_time ):
   next_timestamp = None

   for i in xrange(0, len(rec.opens)):
      if rec.opens[i] > cur_time and (rec.opens[i] < next_timestamp or next_timestamp == None):
         next_timestamp = rec.opens[i]
         break

   for i in xrange(0, len(rec.closes)):
      if rec.closes[i] > cur_time and (rec.closes[i] < next_timestamp or next_timestamp == None):
         next_timestamp = rec.closes[i]
         break
      if next_timestamp < rec.closes[i]:
         break

   for i in xrange(0, len(rec.reads)):
      if rec.reads[i][0] > cur_time and (rec.reads[i][0] < next_timestamp or next_timestamp == None):
         next_timestamp = rec.reads[i][0]
         break
      if next_timestamp < rec.reads[i][0]:
         break

   for i in xrange(0, len(rec.writes)):
      if rec.writes[i][0] > cur_time and (rec.writes[i][0] < next_timestamp or next_timestamp == None):
         next_timestamp = rec.writes[i][0]
         break
      if next_timestamp < rec.writes[i][0]:
         break

   return next_timestamp

   
# print the I/O operations by time
def print_by_order():
   global path_map

   colwidth = 0
   latest_op_time = {}

   paths = path_map.keys()
   paths.sort()

   for filename in paths:
      colwidth = max(colwidth, len(filename) + 5)

      latest_op_time[ get_next_timestamp( path_map[filename], None ) ] = filename

   while True:
      # find the earliest timestamp in latest_op_time
      cur_time = min( latest_op_time.keys() )
      filename = latest_op_time[ cur_time ]
      rec = path_map[filename]

      # what's the index of this filename?
      path_idx = paths.index(filename)
      op_idx = -1
      op = ''

      for i in xrange( 0, len(rec.reads) ):
         timestamp, _, _ = rec.reads[i]
         if timestamp == cur_time:
            op_idx = i
            op = 'read'
            break


      if op_idx == -1:
         for i in xrange( 0, len(rec.writes) ):
            timestamp, _, _ = rec.writes[i]
            if timestamp == cur_time:
               op_idx = i
               op = 'write'
               break

      if op_idx == -1:
         for i in xrange( 0, len(rec.opens) ):
            if rec.opens[i] == cur_time:
               op_idx = i
               op = 'open'
               break

      if op_idx == -1:
         for i in xrange( 0, len(rec.closes) ):
            if rec.closes[i] == cur_time:
               op_idx = i
               op = 'close'
               break

      if op_idx == -1:
         print 'not found for ' + cur_time
         break
         
      space = ' ' * (path_idx)
      print space + filename

      del latest_op_time[ cur_time ]

      # update the latest op time of this file
      if op == 'read':
         print space + 'READ %s-%s' % (rec.reads[op_idx][1], rec.reads[op_idx][2])

      elif op == 'write':
         print space + 'WRITE %s-%s' % (rec.writes[op_idx][1], rec.writes[op_idx][2])

      elif op == 'open':
         print space + 'OPEN'

      elif op == 'close':
         print space + 'CLOSE'

      next_timestamp = get_next_timestamp( rec, cur_time )

      if next_timestamp != None:
         latest_op_time[ next_timestamp ] = filename
         
      # break if there's nothing more to do
      if len(latest_op_time.keys()) == 0:
         break

   


if __name__ == "__main__":
   strace_fd = open( sys.argv[1], "r" )

   line_count = 0
   while True:
      line = strace_fd.readline()

      if len(line) == 0:
         break

      line = line.strip()

      try:
         syscall = parse_strace_line( line )
      except Exception, e:
         print >> sys.stderr, "Failed parsing at line %s" % line_count
         #traceback.print_exc()
         line_count += 1
         continue
      
         #sys.exit(1)

      line_count += 1
      
      if syscall != None:
         if syscall.name == 'open' or syscall.name == 'creat':
            open_file( syscall )
         elif syscall.name == 'read':
            read_file( syscall )
         elif syscall.name == 'write':
            write_file( syscall )
         elif syscall.name == 'close':
            close_file( syscall )
         elif syscall.name == 'lseek':
            lseek_file( syscall )
         elif syscall.name == 'chdir':
            chdir( syscall )
         elif syscall.name == 'fork':
            fork( syscall )
         else:
            print >> sys.stderr, "main: unknown syscall %s" % syscall


   print_by_file()
   #print_by_order()

      
   
   