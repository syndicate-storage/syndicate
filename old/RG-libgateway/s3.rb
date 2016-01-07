#!/usr/bin/env ruby

=begin

   Copyright 2011 Jude Nelson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

=end

require 'rubygems'
require 'right_aws'
require 'digest/md5'
require 'net/http'
require 'cgi'


module ContentSize
   attr_accessor :size
end

# debug info
$debug_buf = ""
def debug( s )
   begin
      $debug_buf += s.to_s() + "\n"
   rescue
   end
end


# print debug info
def print_debug()
   puts $debug_buf
   $debug_buf = ""
end

# write debug info to a file
def write_debug( f )
   f.puts( $debug_buf )
   $debug_buf = ""
end

# bad request
def bad_request()
   puts "Status: 400"
   puts "Content-type: text/plain\n\n"
   puts "Bad request"
   #puts
   #print_debug()
   exit(400)
end


# internal server error
def internal_server_error()
   puts "Status: 500"
   puts "Content-type: text/plain\n\n"
   puts "Internal server error"
   
   puts "Debug trace:"
   write_debug( $stderr )
   puts "End debug trace"
   exit(500)
end
   
$s3_interface = nil

# connect to AWS S3
def connect_s3( access_key, secret_key )
   begin
      $s3_interface = RightAws::S3Interface.new( access_key, secret_key )
      return true
   rescue Exception => e
      $stderr.puts( "connect_s3: exception in connecting to AWS (exception: " + e.to_s() + ")" )
      return false
   end
end



# since the Ruby CGI library is stupid and will try to read the whole PUT'ed file into RAM if initialized, do this manually
request_method = ENV[ "REQUEST_METHOD" ]
query_string = CGI::unescape( ENV[ "QUERY_STRING" ] )
path_info = CGI::unescape( ENV[ "PATH_INFO" ] )
path_trans = CGI::unescape( ENV[ "PATH_TRANSLATED" ] )
server_name = ENV[ "SERVER_NAME" ]
content_len = ENV[ "CONTENT_LENGTH" ].to_i

path_info[0] = ' '
path_trans[0] = ' '
path_info = path_info.strip
path_trans = path_trans.strip

# remove the first directory from path_info, since it's the S3 gateway alias
path_info = path_info.sub( /^[^\/]*\//, "")

debug( "Content-type: text/plain\n\n" )
debug( "server name: " + server_name )
debug( "request    : " + request_method )
debug( "path info  : " + path_info )
debug( "path trans : " + path_trans )
debug( "content len: " + content_len.to_s() )
debug( "query str  : " + query_string )

# access key
user_access_key = nil

# secret key
user_secret = nil

# the user's top-level bucket
user_bucket = nil

debug( "cgi arguments:" )

cgi_args = query_string.split("&")
for arg in cgi_args:
   key, val = arg.split("=")
   debug( "  '" + key.to_s + "' = '" + val.to_s + "'" )
   
   if key == "access"
      user_access_key = val.to_s()
   end
   
   if key == "secret"
      user_secret = val.to_s()
   end
   
   if key == "bucket"
      user_bucket = val.to_s();
   end
   
end


# if any of the required parameters are nil, then error
if user_access_key == nil or user_secret == nil or user_bucket == nil
   bad_request()
end

debug( "user access key: '" + user_access_key + "'" )
debug( "user_secret    : '" + user_secret + "'" )
debug( "user_bucket    : '" + user_bucket + "'" )

# if we got this far, then connect to S3
if not connect_s3( user_access_key, user_secret )
   debug("connect_s3() failed")
   internal_server_error()
end

debug( "Connected to AWS S3" )

# carry out the request, based on whether or not this is a GET or a PUT
if request_method == "GET"
   
   begin
      
      content_type = false
      
      $s3_interface.get( user_bucket, path_info ) { |chunk|
         
         if content_type == false
            content_type = true
            puts "Status: 200"
            puts "Content-type: application/octet-stream\n\n"
         end
         
         # if we get here, then the object was found
         puts chunk
      }
      
   rescue Exception => e
      $stderr.puts( "main: could not read object '" + path_info + "' from bucket '" + user_bucket + "', exception (" + e.to_s + ")" )
      write_debug( $stderr )
   end
   
elsif request_method == "PUT"
   
   begin
      
      $stdin.extend( ContentSize )
      $stdin.size = content_len
      
      $s3_interface.put( user_bucket, path_info, $stdin )
      
      puts "Status: 200"
      puts "Content-type: text/plain\n\n"
      
      # put the new URL
      puts "s3.amazonaws.com/" + user_bucket + "/" + path_info
      
   rescue Exception => e
      $stderr.puts( "main: could not send object to bucket '" + user_bucket + "' under key '" + path_info + "', exception (" + e.to_s + ")" )
      write_debug( $stderr )
   end
   
end

