#!/usr/bin/python

import sys
from types import *
import traceback

__output_stream = None

def init(output_stream=None, reinit=False):
   global __output_stream
   if __output_stream != None and not reinit:
      return
      
   if output_stream==None:
      __output_stream=sys.stdout
   
   elif type(output_stream) == StringType:
      __output_stream = open(output_stream,"a")
   
   else:
      __output_stream = output_stream
   

def info( msg ):
   lines = msg.split("\n")
   
   for l in lines:
      __output_stream.write( "INFO: %s\n" % (l) )



def warn( msg ):
   lines = msg.split("\n")
   
   for l in lines:
      __output_stream.write( "WARN: %s\n" % (l) )
   
   
def error( msg ): 
   lines = msg.split("\n")
   
   for l in lines:
      __output_stream.write( "ERR: %s\n" % (l) )


def critical( msg ):
   lines = msg.split("\n")
   
   for l in lines:
      __output_stream.write( "CRIT: %s\n" % (l) )


def exception( e, msg ):
   lines = msg.split("\n")
   
   for l in lines:
      __output_stream.write( "EXCEPTION: %s\n" % (l) )
   
   traceback.print_exc( e, __output_stream )
   
   
def flush():
   __output_stream.flush()