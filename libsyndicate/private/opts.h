/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// libsyndicate-private header for options.
// Kept private to prevent direct access to this structure
// by files and programs that aren't part of libsyndicate.so.

#ifndef _SYNDICATE_OPTS_PRIVATE_H_
#define _SYNDICATE_OPTS_PRIVATE_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/storage.h"

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 
#endif 

// command-line options
struct md_opts {
   char* config_file;
   char* username;
   char* volume_name;
   char* ms_url;
   char* gateway_name;
   int debug_level;
   bool foreground;
   
   // not set by the parser 
   bool client;
   bool ignore_driver;  // if true, no attempt to load the driver will be made
   uint64_t gateway_type;

   char const* driver_exec_str;
   char const** driver_roles;
   size_t num_driver_roles;
};

#endif
