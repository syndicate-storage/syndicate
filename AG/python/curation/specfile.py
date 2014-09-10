#!/usr/bin/python 

"""
   Copyright 2014 The Trustees of Princeton University

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


import collections

import lxml
import lxml.etree as etree

AG_file_data = collections.namedtuple("AG_file_data", ["path", "reval_sec", "driver", "perm", "query_string"] )
AG_dir_data = collections.namedtuple("AG_dir_data", ["path", "reval_sec", "driver", "perm"] )

class AGCurationException(Exception):
   pass


def make_file_data( path, reval_sec, driver_name, perm, query_string ):
   return AG_file_data( path=path, reval_sec=reval_sec, driver=driver_name, perm=perm, query_string=query_string )


def make_dir_data( path, reval_sec, driver_name, perm ):
   return AG_dir_data( path=path, reval_sec=reval_sec, driver=driver_name, perm=perm )


def is_parent( parent_path, path ):
   """
   Is a path the parent path of another path?
   /a/b is the parent of /a/b/c, but 
   /a/b is not the parent of /a/b/c/d or /a/e or /f.
   """
   pp = parent_path.strip("/")
   p = path.strip("/")
   
   if not p.startswith( pp ):
      return False 
   
   if "/" in pp[len(p):]:
      return False 
   
   return True


def is_directory( data ):
   """
   Is the given named tuple an AG_dir_data?
   """
   try:
      return data.__class__.__name__ == "AG_dir_data"
   except:
      return False 
   
   
# validate a hierarchy dict 
def validate_hierarchy( hierarchy_dict ):
   """
   Validate a dictionary that describes an AG's hierarchy.
   Return True if valid; raise an Exception if not.
   """
   
   # a hierarchy is valid if:
   # * directories map to AG_dir_data instances 
   # * files map to AG_file_data instances
   # * directories have zero or more children 
   # * files have zero children 
   # * each entry except root has an ancestor
   
   # find root 
   root = hierarchy_dict.get("/", None)
   if root is None:
      raise AGCurationException("No root entry defined")
   
   if root.__class__.__name__ != "AG_dir_data":
      raise AGCurationException("Root is not a directory")
   
   # iterate through the paths in lexical order.
   # Then, children follow their ancestors.
   paths = hierarchy_dict.keys()
   paths.sort()
   
   ancestor_paths = []
   ancestor_data = []
   
   for path in paths:
      
      if path in ancestor_paths:
         # duplicate 
         raise AGCurationException("Duplicate path: %s" % path)
      
      last_ancestor = "/" if len(ancestor_paths) == 0 else ancestor_paths[-1] 
      last_ancestor_data = root if len(ancestor_data) == 0 else ancestor_data[-1]
      
      if is_parent( last_ancestor, path ):
         
         # ancestor must be a directory 
         if not is_directory( last_ancestor_data ):
            raise AGCurationException("Not a directory: %s" % last_ancestor)
         
         ancestor_paths.append( path )
         ancestor_data.append( heirarchy_dict[path] )
         
      else:
         
         # pop back to ancestor 
         while len(ancestor_paths) > 0:
            
            ancestor_path = ancestor_paths.pop()
            ancestor_data = ancestor_data.pop()
            
            if is_parent( ancestor_path, path ):
               
               ancestor_paths.append( path )
               ancestor_data.append( hierarchy_dict[path] )
               
               break
            
            
   return True
   

# generate XML output 
def generate_specfile( config_dict, hierarchy_dict ):
   """
   Generate an AG specfile, given a dictionary that 
   maps file paths to AG_file_data instances and 
   directory paths to AG_dir_data instances, as well 
   as a dictionary that maps driver config keys to values.
   
   Return a string that contains the specfile.
   
   WARNING: No validation is performed on hierarchy_dict.
   Use validate_hierarchy() for this.
   """
   
   root = etree.Element("Map")
   
   # add config 
   config = etree.Element("Config")
   
   for (config_key, config_value) in config_dict.items():
      ent = etree.Element( config_key )
      ent.text = config_value
      
      config.append( ent )
      
   root.append( config )
   
   paths = hierarchy_dict.keys()
   paths.sort()
   
   for path in paths:
      
      h = hierarchy_dict[path]
      ent = None 
      query = None
      
      if h.__class__.__name__ == "AG_file_data":
         # this is a file 
         ent = etree.Element("File", perm=oct(h.perm) )
         query = etree.Element("Query", type=h.driver )
         
         query.text = h.query_string
                               
      else:
         # this is a directory 
         ent = etree.Element("Dir", perm=oct(h.perm) )
         query = etree.Element("Query", type=h.driver )

      ent.text = h.path
      
      pair = etree.Element("Pair", reval=h.reval_sec)
      
      pair.append( ent )
      pair.append( query )
      
      root.append( pair )
      
   return etree.tostring( root, pretty_print=True )
