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

    Given below is a file to shell command mapping XML.
    
    <?xml version="1.0"?>
    <Map>
        <Config>
            <DSN>/tmp/shell-driver-cache</DSN>
        </Config>
        <Pair reval="10s 5m">
            <File perm="740">/foo/bar</File>
            <Query type="shell">/usr/bin/wget -q -O - http://www.google.com</Query>
        </Pair>
    </Map>
    
    <Map> Tag
    ---------
    Everything should be enclosed with <Map> tags. Within <Map> tags there can be any number of <Pair> tags but only
    one <Config> tag.

    <Config> Tag
    ------------
    <Config> tag contains configuration information for this mapping. At the moment only <DSN> tag is meaningful to
    shell driver.
    
    <DSN> Tag
    ---------
    <DSN> tag encloses the absolute path to the directory where shell drivers caches output from programs it executes. 

    <Pair> Tag
    ----------
    <Pair> glues a file name enclosed in <File> tag to shell command it's mapped to which is enclosed in <Query> tags.
    

    "reval" attribute can specify a timeout value for the mapping. After this amount of time the driver will revalidate 
    this mapping as mentioned in Ivalidations of Mappings section.

    Time can be specified in weeks(w), days(d), hours(h), minutes(m) and seconds(s). Given below are some examples on
    specifying time.

        - Timeout in 30 seconds
            reval="30s"

        - Timeout in 90 seconds
            reval="90s"
            reval="1m 30s"

        - Timeout in 48 weeks and two days
            reval="48w 2d"

    Default value for "reval" attribute is 48 weeks.
    
    <File> Tag
    ----------
    <File> tag encloses the name of the file the command is mapped to. File name should be followed by an absolute 
    path starting from the root (/). It is under this name we publish this mapping to MS.

    "perm" attribute can be associated with <File> tag to specify file permission bits (POSIX). However AGs only support
    read only files, therefore values assigned to "perm" will be overidden by the driver to read only.
    
    <Query> Tag
    -----------
    <Query> tag encloses the command the file is mapped to.
    
    "type" attribute is associated with <Query> tag and it is mandatory to specify "type" as 'shell' when using shell
    driver as in the above example. Behaviour is undefined if "type" is not set to 'shell'.

Build Instructions:

    - scons AG/drivers/sql
    - scons AG-SQL-driver-install
        
