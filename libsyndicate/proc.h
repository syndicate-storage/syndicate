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

// process management 

#ifndef _LIBSYNDICATE_PROC_
#define _LIBSYNDICATE_PROC_

#include "libsyndicate/libsyndicate.h"
#include "libsyndicate/gateway.h"

extern "C" {

struct SG_proc;
struct SG_proc_group;

// allocators and freers
struct SG_proc* SG_proc_alloc( int num_procs );
void SG_proc_free( struct SG_proc* proc );

struct SG_proc_group* SG_proc_group_alloc( int num_groups );
int SG_proc_group_init( struct SG_proc_group* group );
void SG_proc_group_free( struct SG_proc_group* group );

// getters 
pid_t SG_proc_pid( struct SG_proc* p );
char const* SG_proc_exec_arg( struct SG_proc* p );
struct SG_proc** SG_proc_group_freelist( struct SG_proc_group* group );

int SG_proc_stdin( struct SG_proc* p );
int SG_proc_stdout( struct SG_proc* p );
FILE* SG_proc_stdout_f( struct SG_proc* p );

// updates
int SG_proc_ensure_updated( struct SG_proc* proc );
int SG_proc_group_reload( struct SG_proc_group* group, char const* exec_str, struct SG_chunk* new_config, struct SG_chunk* new_secrets, struct SG_chunk* new_driver );

// helper management 
int SG_proc_start( struct SG_proc* proc, char const* exec_path, char const* exec_arg, char** exec_env, struct SG_chunk* config, struct SG_chunk* secrets, struct SG_chunk* driver );
int SG_proc_stop( struct SG_proc* proc, int timeout );
int SG_proc_kill( struct SG_proc* proc, int signal );
int SG_proc_tryjoin( struct SG_proc* proc, int* child_status );

int SG_proc_group_kill( struct SG_proc_group* proc, int signal );
int SG_proc_group_tryjoin( struct SG_proc_group* group );
int SG_proc_group_stop( struct SG_proc_group* group, int timeout );

// grouping
int SG_proc_group_add( struct SG_proc_group* group, struct SG_proc* proc );
int SG_proc_group_remove( struct SG_proc_group* group, struct SG_proc* proc );
int SG_proc_group_size( struct SG_proc_group* group );

// acquisition/release
struct SG_proc* SG_proc_group_acquire( struct SG_proc_group* group );
int SG_proc_group_release( struct SG_proc_group* group, struct SG_proc* proc );

// locking 
int SG_proc_group_rlock( struct SG_proc_group* group );
int SG_proc_group_wlock( struct SG_proc_group* group );
int SG_proc_group_unlock( struct SG_proc_group* group );

// process communication
int SG_proc_read_int64( FILE* f, int64_t* result );
int SG_proc_read_chunk( FILE* f, struct SG_chunk* chunk );
int SG_proc_write_int64( int fd, int64_t value );
int SG_proc_write_chunk( int out_fd, struct SG_chunk* chunk );
int SG_proc_request_init( struct ms_client* ms, struct SG_request_data* reqdat, SG_messages::DriverRequest* dreq );
int SG_proc_write_request( int fd, SG_messages::DriverRequest* dreq );
bool SG_proc_is_dead( struct SG_proc* proc );

// one-off subprocess in a subshell with bound output 
int SG_proc_subprocess( char const* cmd_path, char* const argv[], char* const env[], char const* input, size_t input_len, char** output, size_t* output_len, size_t max_output, int* exit_status );

}


#endif
