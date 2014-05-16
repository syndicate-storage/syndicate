#!/usr/bin/python

"""
   Copyright 2013 The Trustees of Princeton University

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


def read_file( filename, outfile, **kw ):
   import traceback
   import os
   import errno 
   
   context = kw['context'] 
   STORAGE_DIR = context.config['STORAGE_DIR']
   
   if not os.path.exists( STORAGE_DIR ):
      os.mkdir( STORAGE_DIR )
   
   storage_path = os.path.join(STORAGE_DIR, filename)
   
   fd = None 
   
   try:
      fd = open( storage_path, "r" )
      outfile.write( fd.read() )
      
   except OSError, oe:
      if oe.errno == errno.ENOENT:
         return 404 
      else:
         return 500
      
   except Exception, e:
      context.log.exception(e)
      return 500
   
   finally:
      if fd is not None:
         try:
            fd.close()
         except:
            pass
         
   return 200



def write_file( filename, infile, **kw ):
   import traceback
   import os
   import errno 
   
   buf = infile.read()
   
   context = kw['context']
   STORAGE_DIR = context.config['STORAGE_DIR']
   
   if not os.path.exists( STORAGE_DIR ):
      os.mkdir( STORAGE_DIR )
   
   storage_path = os.path.join(STORAGE_DIR, filename)
   
   fd = None
   try:
      fd = open( storage_path, "w" )
      fd.write( buf )
      
   except OSError, oe:
      if oe.errno == errno.ENOENT:
         return 404 
      else:
         return 500
   
   except Exception, e:
      context.log.exception(e)
      return 500
   
   finally:
      if fd is not None:
         try:
            fd.close()
         except:
            pass
         
   return 200


def delete_file( filename, **kw ):
   import traceback
   import os
   import errno

   context = kw['context']   
   STORAGE_DIR = context.config['STORAGE_DIR']
   
   if not os.path.exists( STORAGE_DIR ):
      os.mkdir( STORAGE_DIR )
   
   storage_path = os.path.join(STORAGE_DIR, filename)
   
   try:
      os.unlink( os.path.join(STORAGE_DIR, filename) )
   
   except OSError, oe:
      if oe.errno == errno.ENOENT:
         return 404 
      else:
         return 500
      
   except Exception, e:
      context.log.exception(e)
      return 500
   
   return 200

if __name__ == "__main__":
   # TODO: do some tests
   pass
