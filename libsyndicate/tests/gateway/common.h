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

#ifndef _LIBSYNDICATE_GATEWAY_TEST_COMMON_H_
#define _LIBSYNDICATE_GATEWAY_TEST_COMMON_H_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/opts.h"
#include "libsyndicate/client.h"
#include "libsyndicate/ms/ms-client.h"

extern "C" {
   
int common_parse_opts( struct md_opts* opts, int argc, char** argv, int* optind );

int common_print_request( SG_messages::Request* request );
int common_print_reply( SG_messages::Reply* reply );

}

#endif