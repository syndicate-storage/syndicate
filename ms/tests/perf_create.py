#!/usr/bin/python

import MS
import storage.storage as storage
import storagetypes
import time

class TestRecord( storagetypes.Object ):
   test_value = storagetypes.String()

def test( ignore1, args ):
   name = "create_test"
   value = "create_test"
   
   if args.has_key('name'):
      name = args['name']

   if args.has_key('value'):
      value = args['value']

   t1 = time.time()
   record = storagetypes.make_key( "test:" + name ).get()
   if record == None:
      record = TestRecord( test_value = 
      record.test_value = value
      record.put()
   


   