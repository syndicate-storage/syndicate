#include "iFuseLib.Http.h"
#include "iFuseLib.Logging.h"

// get the public IP address of this host, and put the address into addr.  return 0 on success; negative on failure
static int http_get_addr( char const* hostname, int portnum, struct sockaddr* addr ) {

   struct addrinfo hints;
   memset( &hints, 0, sizeof(hints) );
   hints.ai_family = AF_UNSPEC;
   hints.ai_flags = AI_CANONNAME;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_protocol = 0;
   hints.ai_canonname = NULL;
   hints.ai_addr = NULL;
   hints.ai_next = NULL;

   int rc = 0;
   struct addrinfo* result = NULL;

   // get the address information from the hostname
   rc = getaddrinfo( hostname, NULL, &hints, &result );
   if( rc != 0 ) {
      // could not get addr info
      fprintf(stderr, "iFuseLib.Http: http_get_addr: getaddrinfo: %s\n", gai_strerror( rc ) );
      return -errno;
   }

   // NOTE: there should only be one IP address for this node, but it
   // is possible that it can have more.  Here, we just take the first
   // address given.

   switch( result->ai_family ) {
      case AF_INET:
         // IPv4
         ((struct sockaddr_in*)addr)->sin_addr = ((struct sockaddr_in*)result->ai_addr)->sin_addr;
         ((struct sockaddr_in*)addr)->sin_port = htons( portnum );
         addr->sa_family = result->ai_family;
         break;

      case AF_INET6:
         // IPv6
         ((struct sockaddr_in6*)addr)->sin6_addr = ((struct sockaddr_in6*)result->ai_addr)->sin6_addr;
         ((struct sockaddr_in6*)addr)->sin6_port = htons( portnum );
         addr->sa_family = result->ai_family;
         break;

      default:
         fprintf(stderr, "iFuseLib.Http: http_get_addr: unknown socket address family %d\n", result->ai_family );
         rc = -1;
         break;
   }

   freeaddrinfo( result );

   return rc;
}


static int http_send_all( int socket_fd, char const* buf, size_t buf_len ) {

   size_t num_sent = 0;
   while( num_sent < buf_len ) {

      ssize_t ns = send( socket_fd, buf + num_sent, buf_len - num_sent, MSG_NOSIGNAL );
      if( ns < 0 ) {
         int errsv = -errno;
         fprintf(stderr, "iFuseLib.Http: http_send_all: send rc = %d\n", errsv );
         return errsv;
      }

      num_sent += ns;
   }

   return 0;
}

// send HTTP upload headers 
static int http_upload_send_headers( int socket_fd, size_t num_bytes ) {
   char const* header_fmt = "\
POST /irodsfs.log.gz HTTP/1.0\r\n\
User-Agent: iFuseLib.Http\r\n\
Content-Type: application/octet-stream\r\n\
Content-Length: %zu\r\n\
\r\n";

   size_t header_buf_len = strlen(header_fmt) + 20;
   char* header_buf = (char*)alloca( header_buf_len );
   memset( header_buf, 0, header_buf_len );
   
   snprintf( header_buf, header_buf_len - 1, header_fmt, num_bytes );

   // send it off
   int rc = http_send_all( socket_fd, header_buf, strlen(header_buf) );
   if( rc != 0 ) {
      fprintf(stderr, "iFuseLib.Http: http_send_all rc = %d\n", rc );
      return rc;
   }

   return 0;
}


// get HTTP acknowledgement 
static int http_upload_get_status( int socket_fd ) {

   char buf[16384];
   memset( buf, 0, 16384 );

   int status = 0;

   while( status == 0 ) {
      ssize_t nr = recv( socket_fd, buf, 16383, 0);
      if( nr < 0 ) {
         int errsv = -errno;
         fprintf(stderr, "iFuseLib.Http: recv rc = %d\n", errsv );
         return errsv;
      }

      char* saveptr = NULL; 
      char* bufptr = buf;
      
      // find HTTP response
      while( 1 ) {
         char* line = strtok_r( bufptr, "\r\n", &saveptr );
         if( line == NULL )
            break;

         bufptr = NULL;

         int http_version = 0;
         
         int rc = sscanf( line, "HTTP/1.%d %d", &http_version, &status );
         if( rc == 2 ) {
            break;
         }
      }
   }

   return status;
}

   

// upload from a file descriptor to a socket.  Goes to EOF or num_bytes
int http_upload( int socket_fd, int file_fd, size_t num_bytes ) {
   
   size_t sent_so_far = 0;
   char buf[4096];

   // send the headers off
   int rc = http_upload_send_headers( socket_fd, num_bytes );
   if( rc != 0 ) {
      fprintf(stderr, "iFuseLib.Http: http_upload_send_headers rc = %d\n", rc );
      return rc;
   }

   lseek( file_fd, 0, SEEK_SET );
   
   while( sent_so_far < num_bytes ) {
      // get data
      ssize_t num_read = read( file_fd, buf, 4096 );

      if( num_read < 0 ) {
         int errsv = -errno;
         fprintf(stderr, "iFuseLib.Http: http_upload: read rc = %d\n", errsv );
         return errsv;
      }

      if( num_read == 0 ) {
         // EOF
         fprintf(stderr, "iFuseLib.Http: http_upload: EOF\n"); 
         return -ERANGE;
      }

      // send
      int rc = http_send_all( socket_fd, buf, num_read ); 
      if( rc < 0 ) {
         fprintf(stderr, "iFuseLib.Http: http_upload: http_send_all rc = %d\n", rc );
         return rc;
      }

      sent_so_far += num_read;
   }

   int status = http_upload_get_status( socket_fd );
   if( status < 0 ) {
       fprintf(stderr, "iFuseLib.Http: http_upload: http_upload_get_status rc = %d\n", status );
       return status;
   }

   return status;
}

// connect via HTTP.  return a socket, or negative error code
int http_connect( char const* hostname, int portnum, int timeout ) {

   struct sockaddr_storage addr;
   memset( &addr, 0, sizeof(addr) );

   int rc = 0;

   // find out more about this host
   rc = http_get_addr( hostname, portnum, (struct sockaddr*)&addr );
   if( rc != 0 ) {
      fprintf(stderr, "iFuseLib.Http: http_connect: http_get_addr(%s) rc = %d\n", hostname, rc );
      return -1;
   }

   // open a connection...
   int socket_fd = socket( addr.ss_family, SOCK_STREAM, 0 );
   if( socket_fd < 0 ) {
      rc = -errno;
      fprintf(stderr, "iFuseLib.Http: http_connect: socket(%d) rc = %d\n", addr.ss_family, rc );
      return rc;
   }

   // set timeouts 
   struct timeval tv;
   tv.tv_sec = timeout;
   tv.tv_usec = 0;
   
   rc = setsockopt( socket_fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&tv, sizeof(struct timeval) );
   if( rc != 0 ) {
      
      rc = -errno;
      fprintf(stderr, "iFuseLib.Http: setsockopt(%d, SO_RCVTIMEO, %d) errno = %d\n", socket_fd, timeout, rc );
      
      close( socket_fd );
      return rc;
   }
   
   rc = setsockopt( socket_fd, SOL_SOCKET, SO_SNDTIMEO, (char*)&tv, sizeof(struct timeval) );
   if( rc != 0 ) {
      
      rc = -errno;
      fprintf(stderr, "iFuseLib.Http: setsockopt(%d, SO_SNDTIMEO, %d) errno = %d\n", socket_fd, timeout, rc );
      
      close( socket_fd );
      return rc;
   }
   
   // connect
   rc = connect( socket_fd, (struct sockaddr*)&addr, sizeof(addr) );
   if( rc < 0 ) {
      rc = -errno;
      fprintf(stderr, "iFuseLib.Http: connect(%d) rc = %d\n", socket_fd, rc );
      close( socket_fd );
      return rc;
   }

   return socket_fd;
}


// sync all log data 
int http_send_log( char const* hostname, int portnum, int timeout, char const* compressed_logfile_path ) {
   
   int rc = 0;
   
   // send the log off 
   int soc = http_connect( hostname, portnum, timeout );
   if( soc < 0 ) {
      
      rc = -errno;
      fprintf(stderr, "failed to connect to %s:%d, rc = %d\n", hostname, portnum, soc );
   }
   else {
      
      FILE* compressed_logfile_f = fopen( compressed_logfile_path, "r" );
      if( compressed_logfile_f == NULL ) {
         
         rc = -errno;
         fprintf(stderr, "failed to open %s\n", compressed_logfile_path );
      }
      else {
         
         // get the size
         int fd = fileno( compressed_logfile_f );
         
         struct stat sb;
         int rc = stat( compressed_logfile_path, &sb );
         if( rc != 0 ) {
            
            rc = -errno;
            fprintf(stderr, "failed to stat %s, rc = %d\n", compressed_logfile_path, rc );
         }
         else {
            
            rc = http_upload( soc, fd, sb.st_size );
            if( rc < 0 ) {
               
               fprintf(stderr, "failed to upload, rc = %d\n", rc );
            }
            if( rc != 200 ) {
               
               fprintf(stderr, "failed to upload, HTTP status = %d\n", rc );
            }
         }

         fclose( compressed_logfile_f );
      }
      
      close( soc );
   }
   
   return rc;
}


// sync all logs to the log server 
// remove from the sync_buf all logs that were successfully uploaded, and preserve the ones that were not.
// return 0 on success, -EAGAIN if some logs failed to be uploaded
int http_sync_all_logs( struct log_context* ctx ) {

   int rc = 0;
   
   // get the list of paths we're going to sync 
   pthread_rwlock_wrlock( &ctx->lock );
   
   log_sync_buf_t* compressed_logs = ctx->sync_buf;
   ctx->sync_buf = new log_sync_buf_t();
   
   pthread_rwlock_unlock( &ctx->lock );
   
   // keep track of failed logs, so we can try again 
   log_sync_buf_t failed;
   
   for( unsigned int i = 0; i < compressed_logs->size(); i++ ) {
      
      // next log...
      char* compressed_logfile_path = compressed_logs->at(i);
   
      // remove from our listing (so we don't free it)
      (*compressed_logs)[i] = NULL;
      
      // sync the log 
      rc = http_send_log( ctx->hostname, ctx->portnum, ctx->timeout, compressed_logfile_path );
      if( rc != 0 ) {
         
         fprintf(stderr, "http_send_log(%s:%d, %s) rc = %d\n", ctx->hostname, ctx->portnum, compressed_logfile_path, rc );
         
         failed.push_back( compressed_logfile_path );
         continue;
      }
      
      // sent!  we're done with this!
      unlink( compressed_logfile_path );
      free( compressed_logfile_path );
   }
   
   delete compressed_logs;
   
   rc = 0;
   if( failed.size() > 0 ) {
      
      // try these again 
      pthread_rwlock_wrlock( &ctx->lock );
      
      for( unsigned int i = 0; i < failed.size(); i++ ) {
         
         ctx->sync_buf->push_back( failed[i] );
      }
      
      pthread_rwlock_unlock( &ctx->lock );
      
      rc = -EAGAIN;
   }
   
   return rc;
}


// thread to continuously sync log data 
void* http_sync_log_thread( void* arg ) {
   
   struct log_context* ctx = (struct log_context*)arg;
   int rc = 0;
   
   // since we don't hold any resources between uploads, simply cancel immediately
   pthread_setcanceltype( PTHREAD_CANCEL_ASYNCHRONOUS, NULL );
   
   while( ctx->running ) {
      
      // wait for new logs to appear 
      sem_wait( &ctx->sync_sem );
      
      // upload, but don't allow for interruption 
      pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, NULL );
      
      rc = http_sync_all_logs( ctx );
      if( rc != 0 ) {
         fprintf(stderr, "WARN: http_sync_all_logs rc = %d\n", rc );
      }
      
      // re-enable interrupts
      pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, NULL );
   }
   
   return NULL;
}

