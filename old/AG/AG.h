/*
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
*/

#ifndef _AG_H
#define _AG_H_


#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/ms/ms-client.h"

// basic types 
typedef map<string, string> AG_config_t;

// map query types to drivers
typedef map<string, struct AG_driver*> AG_driver_map_t;

// map path to AG_map_info 
typedef map<string, struct AG_map_info*> AG_fs_map_t;


#endif
