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
#include "core.h"
#include "events.h"

// global signal handler 
struct AG_signal_listener g_sigs;

int AG_handle_event( struct AG_event_listener* event_listener, int event_type, char* payload );

// add an event handler to a dispatch table 
static int AG_set_event_handler( int event, AG_event_handler handler, void* arg, AG_event_handler* handler_list, void** arg_list ) {
   
   if( event < 0 || event >= AG_NUM_EVENTS ) {
      return -EINVAL;
   }
   
   handler_list[ event ] = handler;
   arg_list[ event ] = arg;
   
   return 0;
}

// add an event handler
int AG_add_event_handler(struct AG_event_listener* events, int event, AG_event_handler handler, void *args) {
   
   // sanity check 
   if (handler == NULL) {
      return -EINVAL;
   }
   
   int rc = AG_set_event_handler( event, handler, args, events->handlers, events->args );
   
   if( rc != 0 ) {
      errorf("AG_set_event_handler(%d, %p, %p) rc = %d\n", event, handler, args, rc );
   }
   
   return rc;
}


// remove an event handler 
int AG_remove_event_handler(struct AG_event_listener* events, int event) {
   
   // sanity check 
   if( event < 0 || event >= AG_NUM_EVENTS ) {
      return -EINVAL;
   }
   
   int rc = AG_set_event_handler( event, NULL, NULL, events->handlers, events->args );
   
   if( rc != 0 ) {
      errorf("AG_set_event_handler(%d, NULL, NULL) rc = %d\n", event, rc );
   }
   
   return rc;
}


// add a signal handler
int AG_add_signal_handler( int signum, sighandler_t sighandler ) {
   
   if( g_sigs.signal_running ) {
      return -EINPROGRESS;
   }
   
   if( (*g_sigs.signal_map)[ signum ].find( sighandler ) != (*g_sigs.signal_map)[ signum ].end() ) {
      return -EPERM;
   }
   
   (*g_sigs.signal_map)[ signum ].insert( sighandler );
   
   return 0;
}

// remove a signal handler 
int AG_remove_signal_handler( int signum, sighandler_t sighandler ) {
   
   if( g_sigs.signal_running ) {
      return -EINPROGRESS;
   }
   
   if( (*g_sigs.signal_map)[ signum ].find( sighandler ) == (*g_sigs.signal_map)[ signum ].end() ) {
      return -EPERM;
   }
   
   (*g_sigs.signal_map)[ signum ].erase( sighandler );
   
   return 0;
}


// our actual signal handler, which gets multiplexed 
void AG_sighandler( int signum ) {
   ssize_t rc = write( g_sigs.signal_pipe[1], &signum, sizeof(int) );
   if( rc != 0 ) {
      rc = -errno;
      errorf("write(signalpipe) errno = %zd\n", rc);
   }
}


// signal multiplexer 
void* AG_signal_listener_main_loop( void* arg ) {
   
   while( g_sigs.signal_running ) {
      
      // next signal 
      int next_signal = 0;
      
      // read uninterrupted
      ssize_t num_read = 0;
      ssize_t rc = 0;
      bool die = false;
      
      while( (unsigned)num_read < sizeof(int) ) {
         
         rc = read( g_sigs.signal_pipe[0], (char*)(&next_signal) + num_read, sizeof(int) - num_read );
         
         if( rc < 0 ) {
            rc = -errno;
            
            if( rc == -EINTR && !g_sigs.signal_running ) {
               // asked to die 
               die = true;
               break;
            }
            errorf("read(signalpipe) errno = %zd\n", rc);
            break;
         }
         if( rc == 0 ) {
            break;
         }
         
         num_read += rc;
      }
      
      // asked to die?
      if( die || !g_sigs.signal_running ) {
         break;
      }
      
      // handle this signal 
      AG_signal_map_t::iterator sigset_itr = g_sigs.signal_map->find( next_signal );
      if( sigset_itr == g_sigs.signal_map->end() ) {
         // no handler 
         continue;
      }
      
      // call all handlers 
      const set<sighandler_t>& sighandlers = sigset_itr->second;
      
      for( set<sighandler_t>::iterator itr = sighandlers.begin(); itr != sighandlers.end(); itr++ ) {
         
         sighandler_t handler = *itr;
         
         (*handler)( next_signal );
      }
   }
   
   dbprintf("%s", "AG Signal handler thread exit\n");
   
   return NULL;
}


// read all of a length of data from a file descriptor 
static int AG_read_buf_from_fd( int fd, char* buf, size_t buf_len ) {
   
   ssize_t num_read = 0;
   
   // read the event type
   while( (unsigned)num_read < buf_len ) {
      ssize_t nr = recv( fd, buf + num_read, buf_len - num_read, MSG_NOSIGNAL );
   
      if( nr < 0 ) {
         nr = -errno;
         errorf("read(%d) errno = %zd\n", fd, nr );
         return (int)nr;
      }
      
      num_read += nr;
   }
   
   return 0;
}

// send all of a length of data to a file descriptor 
static int AG_write_buf_to_fd( int fd, char* buf, size_t buf_len ) {
   
   // send the message 
   ssize_t num_sent = 0;
   while( (unsigned)num_sent < buf_len ) {
      
      ssize_t ns = send( fd, buf + num_sent, buf_len - num_sent, MSG_NOSIGNAL );
      if( ns < 0 ) {
         
         ns = -errno;
         errorf("send(%d) rc = %zd\n", fd, ns );
         return (int)ns;
      }
      
      num_sent += ns;
   }
   
   return 0;
}

// get the next event 
// event_payload must be at least AG_EVENT_PAYLOAD_LEN bytes long
static int AG_get_next_event( struct AG_event_listener* event_listener, int32_t* event, char* event_payload ) {

   char* event_buf = (char*)event;
   
   int rc = 0;
   
   // client connection
   int client_sock = 0;
   struct sockaddr_un client_connection;
   socklen_t client_connection_len = sizeof(client_connection);
   
   // accept the next connection 
   client_sock = accept( event_listener->sock_fd, (struct sockaddr*)&client_connection, &client_connection_len );
   if( client_sock != 0 ) {
      rc = -errno;
      errorf("accept(%d) errno = %d\n", event_listener->sock_fd, rc );
      return rc;
   }
   
   // get the event type 
   rc = AG_read_buf_from_fd( client_sock, event_buf, sizeof(int32_t) );
   if( rc != 0 ) {
      errorf("Failed to read event type, rc = %d\n", rc );
      close( client_sock );
      return rc;
   }
   
   // get the event buffer 
   rc = AG_read_buf_from_fd( client_sock, event_payload, AG_EVENT_PAYLOAD_LEN );
   if( rc != 0 ) {
      errorf("Failed to read event payload for event %d, rc = %d\n", *event, rc );
      close( client_sock );
      return rc;
   }
   
   close( client_sock );
   
   // success
   return 0;
}


// main loop for listening for events
void* AG_event_listener_event_loop(void * arg) {
   
   struct AG_event_listener* event_listener = (struct AG_event_listener*)arg;
   
   int rc = 0;
   
   int32_t event_type = 0;
   char event_payload[AG_EVENT_PAYLOAD_LEN];
   
   dbprintf("%s", "AG event listener thread started\n");
   
   while(true) {
      
      // wait for the next event 
      rc = AG_get_next_event( event_listener, &event_type, event_payload );
      if( rc != 0 ) {
         errorf("AG_get_next_event rc = %d\n", rc );
         
         // EBADF? try re-opening 
         if( rc == -EBADF ) {
            // TODO
         }
         else {
            // fatal 
            break;
         }
      }
      
      // process the event 
      rc = AG_handle_event( event_listener, event_type, event_payload );
      if( rc != 0 ) {
         
         // log the error
         errorf("AG_handle_event(%d) rc = %d\n", event_type, rc );
         continue;
      }
   }
   
   dbprintf("%s", "AG event listener thread exit\n");
   return NULL;
}


// initialize an event handler.
int AG_event_listener_init( struct AG_event_listener* event_listener, struct AG_opts* ag_opts ) {
   
   memset( event_listener, 0, sizeof(struct AG_event_listener) );
   event_listener->event_running = false;
   
   int rc = 0;
   int fd = 0;
   
   // initialize unix socket 
   fd = md_unix_socket( ag_opts->sock_path, true );
   if( fd < 0 ) {
      
      errorf("md_unix_socket(%s) rc = %d\n", ag_opts->sock_path, fd );
      
      if( fd == -EADDRINUSE ) {
         // try unlinking, and then try again 
         rc = unlink( ag_opts->sock_path );
         if( rc != 0 ) {
            rc = -errno;
            errorf("unlink(%s) rc = %d\n", ag_opts->sock_path, rc );
            
            return rc;
         }
         else {
            
            errorf("WARN: unlinked %s\n", ag_opts->sock_path );
            
            // succeeded in unlinking.  Try connecting again.
            fd = md_unix_socket( ag_opts->sock_path, true );
            if( fd < 0 ) {
               errorf("After unlinking, md_unix_socket(%s) rc = %d\n", ag_opts->sock_path, fd );
               return fd;
            }
            
            // succeeded by unlinking and re-creating
         }
      }
   }
   
   event_listener->sock_fd = fd;
   event_listener->sock_path = strdup(ag_opts->sock_path);
   
   return 0;
}


// start the event handler 
int AG_event_listener_start( struct AG_event_listener* event_listener ) {
   
   event_listener->event_running = true;
   
   // start listening on it 
   event_listener->event_thread = md_start_thread( AG_event_listener_event_loop, event_listener, false );
   
   if( (int)event_listener->event_thread < 0 ) {
      errorf("md_start_thread rc = %d\n", (int)event_listener->event_thread );
      return (int)event_listener->event_thread;
   }
   
   return 0;
}

// stop the event handler 
int AG_event_listener_stop( struct AG_event_listener* event_listener ) {
   
   // already stopped?
   if( !event_listener->event_running ) 
      return -EINVAL;
   
   event_listener->event_running = false;
   
   dbprintf("%s", "Stopping AG event listener\n");
   
   // cancel and join the thread 
   pthread_cancel( event_listener->event_thread );
   pthread_join( event_listener->event_thread, NULL );
   
   return 0;
}


// clean up the event handler 
int AG_event_listener_free( struct AG_event_listener* event_listener ) {
   
   if( event_listener->event_running ) {
      // need to stop first 
      return -EINVAL;
   }
   
   if( event_listener->sock_fd > 0 ) {
      close( event_listener->sock_fd );
      event_listener->sock_fd = -1;
   }
   
   if( event_listener->sock_path != NULL ) {
      
      int rc = unlink( event_listener->sock_path );
      if( rc != 0 ) {
         
         rc = -errno;
         errorf("ERR: failed to unlink %s, errno = %d\n", event_listener->sock_path, rc );
      }
         
      free( event_listener->sock_path );
      event_listener->sock_path = NULL;
   }
   
   return 0;
}

// set up signal handling 
int AG_signal_listener_init() {
   
   memset( &g_sigs, 0, sizeof(g_sigs) );
   
   int rc = pipe( g_sigs.signal_pipe );
   if( rc != 0 ) {
      rc = -errno;
      errorf("pipe(signalpipe) errno = %d\n", rc );
      return rc;
   }
   
   g_sigs.signal_map = new AG_signal_map_t();
   g_sigs.old_signal_map = new AG_old_signal_map_t();
   g_sigs.signal_running = false;
   return 0;
}


// start signal handling 
int AG_signal_listener_start() { 

   dbprintf("%s", "Starting AG signal handling thread\n");
   
   // register all signals 
   for( AG_signal_map_t::iterator itr = g_sigs.signal_map->begin(); itr != g_sigs.signal_map->end(); itr++ ) {
      
      struct sigaction AG_signal, old_signal;
      memset( &AG_signal, 0, sizeof(struct sigaction) );
      
      AG_signal.sa_handler = AG_sighandler;
      
      int rc = sigaction( itr->first, &AG_signal, &old_signal );
      if( rc != 0 ) {
         
         rc = -errno;
         if( (itr->first == SIGKILL || itr->first == SIGSTOP) && rc == -EINVAL ) {
            errorf("WARN: you cannot catch SIGKILL (%d) or SIGSTOP (%d).  Ignoring this signal handler.\n", SIGKILL, SIGSTOP);
            continue;
         }
         else {
            errorf("sigaction(%d) errno = %d\n", itr->first, rc );
            return rc;
         }
      }
      
      // save this so we can restore it later
      (*g_sigs.old_signal_map)[ itr->first ] = old_signal;
   }
   
   // start the thread 
   g_sigs.signal_running = true;
   
   g_sigs.signal_thread = md_start_thread( AG_signal_listener_main_loop, NULL, false );
   if( (int)(g_sigs.signal_thread) < 0 ) {
      
      int rc = (int)(g_sigs.signal_thread);
      errorf("md_start_thread( AG_signal_listener_main_loop ) rc = %d\n", rc );
      
      g_sigs.signal_running = false;
      return rc;
   }
   
   return 0;
}


// stop signal handling 
int AG_signal_listener_stop() { 
   
   dbprintf("%s", "Stopping AG signal handling thread\n");
   
   g_sigs.signal_running = false;
   
   pthread_cancel( g_sigs.signal_thread );
   pthread_join( g_sigs.signal_thread, NULL );
   
   int rc = 0;
   
   // revert all signal handlers 
   for( AG_old_signal_map_t::iterator itr = g_sigs.old_signal_map->begin(); itr != g_sigs.old_signal_map->end(); itr++ ) {
      
      rc = sigaction( itr->first, &itr->second, NULL );
      if( rc != 0 ) {
         
         rc = -errno;
         errorf("ERR: sigaction(%d) errno = %d\n", itr->first, rc );
      }
   }
   
   return rc;
}


// free the signal handler logic 
int AG_signal_listener_free() {
   
   if( g_sigs.signal_running ) {
      return -EINVAL;
   }
   
   if( g_sigs.signal_map ) {
      delete g_sigs.signal_map;
   }
   if( g_sigs.old_signal_map ) {
      delete g_sigs.old_signal_map;
   }
   
   close( g_sigs.signal_pipe[0] );
   close( g_sigs.signal_pipe[1] );
   
   memset( &g_sigs, 0, sizeof(struct AG_signal_listener) );
   
   return 0;
}


// dispatch an event 
static int AG_dispatch_event( int event_type, char* event_payload, AG_event_handler* handler_list, void** arg_list ) {
   
   int rc = 0;
   
   // look up the preemptive built-in event handler 
   AG_event_handler handler = handler_list[ event_type ];
   void* arg = arg_list[ event_type ];
   
   // invoke the built-in handler first 
   if( handler != NULL ) {
      rc = (*handler)( event_payload, arg );
      if( rc != 0 ) {
         errorf("Event handler %p for event type %d rc = %d\n", handler, event_type, rc );
      }
   }
   
   return rc;
}

// handle an event.
// invoke any built-in handlers first.
// then, invoke the driver's handlers.
int AG_handle_event( struct AG_event_listener* event_listener, int event_type, char* payload ) {
   
   int rc = 0;
   
   // sanity check 
   if( event_type < 0 || event_type >= AG_NUM_EVENTS ) {
      // invalid 
      errorf("Invalid event type %d\n", event_type);
      return -EINVAL;
   }
   
   // dispatch it
   rc = AG_dispatch_event( event_type, payload, event_listener->handlers, event_listener->args );
   
   if( rc != 0 ) {
      errorf("AG event handler for event type %d rc = %d\n", event_type, rc );
      return rc;
   }
   
   return rc;
}

// send an event to an AG, by path to its UNIX socket 
// event_buf cannot be longer than AG_EVENT_PAYLOAD_LEN
int AG_send_event( char const* sock_path, int event_type, char* event_buf, size_t event_buf_len ) {
   
   if( event_buf_len > AG_EVENT_PAYLOAD_LEN ) {
      return -EINVAL;
   }
   
   int rc = 0;
   
   // connect to the socket 
   int sock_fd = md_unix_socket( sock_path, false );
   if( sock_fd < 0 ) {
      
      errorf("md_unix_socket(%s) rc = %d\n", sock_path, sock_fd );
      return sock_fd;
   }
   
   // cast to the proper size 
   int32_t event_type_32 = (int32_t)event_type;
   char event_payload[AG_EVENT_PAYLOAD_LEN];
   
   memset( event_payload, 0, AG_EVENT_PAYLOAD_LEN );
   memcpy( event_payload, event_buf, event_buf_len );
   
   // send the event type 
   rc = AG_write_buf_to_fd( sock_fd, (char*)&event_type_32, sizeof(event_type_32) );
   if( rc != 0 ) {
      errorf("Failed to send event type, rc = %d\n", rc );
      
      close( sock_fd );
      return rc;
   }
   
   // send the event payload 
   rc = AG_write_buf_to_fd( sock_fd, event_payload, AG_EVENT_PAYLOAD_LEN );
   if( rc != 0 ) {
      errorf("Failed to send event payload, rc = %d\n", rc );
      
      close( sock_fd );
      return rc;
   }
   
   close( sock_fd );
   
   return 0;
}


// send a driver ioctl event.
// payload format: query_type:payload
int AG_send_driver_ioctl_event( char const* sock_path, char const* driver_query_type, char* payload, size_t payload_len ) {
   
   size_t driver_query_type_len = strlen(driver_query_type);
   
   // sanity check 
   if( driver_query_type_len + payload_len + 1 > AG_EVENT_PAYLOAD_LEN ) {
      return -EINVAL;
   }
   
   char full_payload[AG_EVENT_PAYLOAD_LEN];
   
   memcpy( full_payload, driver_query_type, driver_query_type_len );
   full_payload[ driver_query_type_len ] = ':';
   memcpy( full_payload + driver_query_type_len + 1, payload, payload_len );
   
   return AG_send_event( sock_path, AG_EVENT_DRIVER_IOCTL_ID, full_payload, AG_EVENT_PAYLOAD_LEN );
}

// parse a driver ioctl event 
// from query_type:payload, extract query_type and payload
int AG_parse_driver_ioctl( char const* msg, char** driver_query_type, char** event_payload, size_t* event_payload_len ) {
   
   // do this safely...
   int sep_off = -1;
   for( int i = 0; i < AG_EVENT_PAYLOAD_LEN; i++ ) {
      
      if( msg[i] == 0 && sep_off < 0 ) {
         // no null allowed in query type part
         return -EINVAL;
      }
      
      if( msg[i] == ':' ) {
         sep_off = i;
         break;
      }
   }
   
   // no ":"
   if( sep_off < 0 ) {
      return -EINVAL;
   }
   
   char* query_type = CALLOC_LIST( char, sep_off + 1 );
   char* payload = CALLOC_LIST( char, AG_EVENT_PAYLOAD_LEN - sep_off );
   
   memcpy( query_type, msg, sep_off - 1 );
   memcpy( payload, msg + sep_off + 1, AG_EVENT_PAYLOAD_LEN - sep_off - 1 );
   
   *event_payload = payload;
   *event_payload_len = AG_EVENT_PAYLOAD_LEN - sep_off - 1;
   *driver_query_type = query_type;
   
   return 0;
}

