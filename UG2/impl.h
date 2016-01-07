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

#ifndef _UG_IMPL_H_
#define _UG_IMPL_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate/gateway.h>


// gateway implementation 
extern "C" {

// set up the gateway 
int UG_impl_install_methods( struct SG_gateway* gateway );

}

#endif