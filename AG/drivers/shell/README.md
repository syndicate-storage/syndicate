Shell Driver
============

AG shell driver maps file names to shell commands and publishes them to volumes attached to the AG.

Process Management:

    AG-Shell driver maintains a process table with process table entries to efficiently serve data block requests. 
    When a file is read  for the  first time,  the driver will add a process table entry to the process table and 
    spawns a process that will run  the shell  command. Since it is inefficient  to fork and execute programs for 
    every block request, the driver runs the process  to completion the  first time a block is requested from the 
    corresponding file and caches that  data in a file. Caching is done by redirecting STDOUT of the process to a 
    file with a random name. The process table entry corresponding to the command will have a pointer to the name 
    of this file.  These  files  are  also  monitored  using  Linux  inotify(2) (inotify is a set of system calls 
    provided by the Linux operating system to monitor the status of a set of files using a single file descriptor). 
    Whenever a change to a file is detected, a separate thread updates the corresponding process table entry with 
    the current size of the file. When a process terminates it is notified to the driver via SIGCHLD signal and 
    the process table entry will be marked as completed. 
    
    Whenever a block is requested, the driver will search for the corresponding process table entry if it is in the 
    table,  block - id  in  the  request  will  be translated to a byte range. If the byte range exists in the file 
    according to the process table entry then bytes in the range will be read from the file and returned to the user. 
    If the byte range is not availableand  the  process table entry indicates that process is still alive an EAGAIN
    will be received by the reader via SyndicateFS as soon as driver sends an HTTP 204 status code to the UG. If the
    byte  range  is  not  available and  the process has terminated driver sends HTTP 404 status code which will be 
    translated to an EOF at the UG end. 

Invalidation of Mappings:

    Data curator can set timeouts to any file name to command mapping. When the mapping times out the process table 
    entry corresponding to the mapping will be reinitialized, the process will be killed if it’s running at that 
    point and the backing store (file the STDOUT of the process is redirected to) will be deleted. 


Mapping XML:

Build Instructions:
    - scons AG/drivers/sql
    - scons AG-SQL-driver-install

