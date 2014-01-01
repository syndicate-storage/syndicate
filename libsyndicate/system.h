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

// system methods

#ifndef _LIBSYNDICATE_SYSTEM_H_
#define _LIBSYNDICATE_SYSTEM_H_

#include "libsyndicate/libsyndicate.h"
#include "util.h"

extern "C" {
   
// daemon
int md_daemonize( char* logfile_path, char* pidfile_path, FILE** logfile );
int md_release_privileges();

// directory manipulation
int md_mkdirs( char const* dirp );
int md_mkdirs2( char const* dirp, int start, mode_t mode );
int md_mkdirs3( char const* dirp, mode_t mode );
int md_rmdirs( char const* dirp );

}

#endif