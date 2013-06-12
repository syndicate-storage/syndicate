#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <limits.h>
#include <errno.h>
#include <netdb.h>

int main( int argc, char** argv ) {
   int rc;
   
   // IPv4 socket
   int sock4 = socket( AF_INET, SOCK_STREAM, 0 );
   
   struct sockaddr_in addr4;
   memset( &addr4, 0, sizeof( struct sockaddr_in) );
   
   addr4.sin_family = AF_INET;
   addr4.sin_addr.s_addr = INADDR_ANY;
   addr4.sin_port = htons( 30000 );
   
   rc = bind( sock4, (struct sockaddr*)&addr4, sizeof(addr4));
   if( rc != 0 ) {
      fprintf(stderr, "Bind on IPv4 socket to INADDR_ANY failed! errno = %d\n", errno );
      exit(1);
   }
   
   close( sock4 );
   
   
   // IPv6 socket
   int sock6 = socket( AF_INET6, SOCK_STREAM, 0 );
   
   struct sockaddr_in6 addr6;
   memset( &addr6, 0, sizeof( struct sockaddr_in6 ) );
   
   addr6.sin6_family = AF_INET6;
   addr6.sin6_addr = in6addr_any;
   addr6.sin6_port = htons( 30000 );
   
   rc = bind( sock6, (struct sockaddr*)&addr6, sizeof(addr6) );
   if( rc != 0 ) {
      fprintf(stderr, "Bind on IPv6 socket to in6addr_any failed! errno = %d\n", errno );
      exit(1);
   }
   
   close( sock6 );
   
   printf("Tests passed!\n");
   return 0;
}