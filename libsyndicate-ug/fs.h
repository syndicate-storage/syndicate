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

#ifndef _UG_FS_H_
#define _UG_FS_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/client.h>
#include <fskit/fskit.h>

#include "core.h"

extern "C" {
   
// link the UG into fskit 
int UG_fs_install_methods( struct fskit_core* fs, struct UG_state* ug );

// unlink the UG from fskit 
int UG_fs_uninstall_methods( struct fskit_core* fs );

// install methods for shutting down
int UG_fs_install_shutdown_methods( struct fskit_core* fs );


}

#endif 
