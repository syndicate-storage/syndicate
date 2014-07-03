#include "iFuseLib.Http.h"

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
int http_connect( char const* hostname, int portnum ) {

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


