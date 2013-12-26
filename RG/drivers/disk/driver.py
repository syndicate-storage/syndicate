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

   context = kw['context'] 
   STORAGE_DIR = context.config['STORAGE_DIR']
   
   try:
      fd = open( os.path.join(STORAGE_DIR, filename), "r" )
      outfile.write( fd.read() )
      fd.close()
   except Exception, e:
      context.log.exception(e)
      return 500
   
   return 200



def write_file( filename, infile, **kw ):
   import traceback

   buf = infile.read()
   
   context = kw['context']
   STORAGE_DIR = context.config['STORAGE_DIR']
   
   try:
      fd = open( os.path.join(STORAGE_DIR, filename), "w" )
      fd.write( buf )
      fd.close()
   except Exception, e:
      context.log.exception(e)
      return 500
   
   return 200


def delete_file( filename, **kw ):
   import traceback
   import os

   context = kw['context']   
   STORAGE_DIR = context.config['STORAGE_DIR']
   
   try:
      os.unlink( os.path.join(STORAGE_DIR, filename) )
   except Exception, e:
      context.log.exception(e)
      return 500
   
   return 200

if __name__ == "__main__":
   # TODO: do some tests
   pass
