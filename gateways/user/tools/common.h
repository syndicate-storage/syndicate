/*
   Copyright 2015 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _SYNDICATE_COMMON_H_
#define _SYNDICATE_COMMON_H_

#include <libsyndicate-ug/client.h>
#include <libsyndicate-ug/core.h>

struct tool_opts {
    
    bool anonymous;        // run as an anonymous user?
};

int print_entry( struct md_entry* dirent );
int parse_args( int argc, char** argv, struct tool_opts* opts );
int usage( char const* progname, char const* args );

#endif
