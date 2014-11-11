/*
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
*/

#ifndef _LIBSYNDICATE_MS_GETATTR_
#define _LIBSYNDICATE_MS_GETATTR_

#include "libsyndicate/ms/core.h"
#include "libsyndicate/ms/file.h"
#include "libsyndicate/ms/url.h"

typedef map<struct md_download_context*, int> ms_client_getattr_downloading_set;

// getattr context 
struct ms_client_getattr_context {
   
   struct ms_client* client;
   
   ms_path_t* path;
   
   vector<int>* to_download;                                           // queue of path indexes to download
   int* attempts;                                                      // attempt counts for each path entry (indexed to path)
   ms_client_getattr_downloading_set* downloading;                     // associate each download context to the ith path entry
   
   struct md_entry* results_buf;                                       // downloaded data for each path entry
   int listing_error;                                                  // MS-given error in processing a request
   int num_downloaded;                                                 // how many entries successfully downloaded
   
   pthread_mutex_t lock;
};

extern "C" {

int ms_client_getattr_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result );
int ms_client_getchild_multi( struct ms_client* client, ms_path_t* path, struct ms_client_multi_result* result );

}

#endif