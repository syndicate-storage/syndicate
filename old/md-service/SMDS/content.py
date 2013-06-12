#!/usr/bin/python

from SMDS.db import Row, Table
import SMDS.logger as logger
from SMDS.parameter import Parameter, Mixed
from SMDS.filter import Filter

from types import *

class Content(Row):
   
   table_name = 'contents'
   primary_key = 'content_id'
   join_tables = ['user_content']
   
   fields = {
      'content_id':             Parameter(int, "Content server identifier"),
      'host_url':               Parameter(str, "Base URL of the content server"),
      'owner':                  Parameter(long, "User ID of the user that is responsible for this content server")
   }
   

class Contents(Table):
   
   def __init__(self, api, content_filter = None, columns = None ):
      Table.__init__(self, api, Content, columns)

      db_name = "contents"
      
      sql = "SELECT %s FROM %s WHERE True" % \
         (", ".join(self.columns.keys()),db_name)

      if content_filter is not None:
         if isinstance(content_filter, (list, tuple, set)):
               # Separate the list into integers and strings
               ints = filter(lambda x: isinstance(x, (int, long)), content_filter)
               strs = filter(lambda x: isinstance(x, StringTypes), content_filter)
               content_filter = Filter(Content.fields, {'content_id': ints, 'host_url': strs})
               sql += " AND (%s) %s" % content_filter.sql(api, "OR")
         elif isinstance(content_filter, dict):
               content_filter = Filter(Content.fields, content_filter)
               sql += " AND (%s) %s" % content_filter.sql(api, "AND")
         elif isinstance (content_filter, StringTypes):
               content_filter = Filter(Content.fields, {'host_url':[content_filter]})
               sql += " AND (%s) %s" % content_filter.sql(api, "AND")
         elif isinstance (content_filter, int):
               content_filter = Filter(Content.fields, {'content_id':[content_filter]})
               sql += " AND (%s) %s" % content_filter.sql(api, "AND")
         else:
               raise MDInvalidArgument, "Wrong content filter %r"%content_filter

      self.selectall(sql)
