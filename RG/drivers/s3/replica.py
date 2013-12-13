#!/usr/bin/python

def replica_read( context, request_info, filename, outfile ):
   return context.drivers['builtin'].read_file( filename, outfile, config=context.config, secrets=context.secrets )

def replica_write( context, request_info, filename, infile ):
   return context.drivers['builtin'].write_file( filename, infile, config=context.config, secrets=context.secrets )

def replica_delete( context, request_info, filename ):
   return context.drivers['builtin'].delete_file( filename, config=context.config, secrets=context.secrets )