#!/usr/bin/python

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
