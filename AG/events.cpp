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
   event_listener->running = false;
   
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
   
   event_listener->running = true;
   
   // start listening on it 
   event_listener->thread = md_start_thread( AG_event_listener_event_loop, event_listener, false );
   
   if( (int)event_listener->thread < 0 ) {
      errorf("md_start_thread rc = %d\n", (int)event_listener->thread );
      return (int)event_listener->thread;
   }
   
   return 0;
}

// stop the event handler 
int AG_event_listener_stop( struct AG_event_listener* event_listener ) {
   
   // already stopped?
   if( !event_listener->running ) 
      return -EINVAL;
   
   event_listener->running = false;
   
   dbprintf("%s", "Stopping AG event listener\n");
   
   // cancel and join the thread 
   pthread_cancel( event_listener->thread );
   pthread_join( event_listener->thread, NULL );
   
   return 0;
}


// clean up the event handler 
int AG_event_listener_free( struct AG_event_listener* event_listener ) {
   
   if( event_listener->running ) {
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

