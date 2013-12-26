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

#ifndef _LOG_H_
#define _LOG_H_

#include "libsyndicate.h"

// logging functions
FILE* log_init(const char* logpath);
int log_shutdown( FILE* logfile );
void logmsg( FILE* logfile, const char* msg, ... );
int logerr( FILE* logfile, const char* msg, ... );

#endif
