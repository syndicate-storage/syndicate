/*
 * Copyright 2013 The Trustees of Princeton University
 * All Rights Reserved
 * 
 * Re-map bind() on 0.0.0.0 or :: to bind() on the node's public IP address
 * Jude Nelson (jcnelson@cs.princeton.edu)
 */

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <limits.h>
#include <errno.h>
#include <netdb.h>
#include <unistd.h>
#include <dlfcn.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <time.h>

int (*bind_original)(int fd, struct sockaddr* addr, socklen_t len ) = NULL;

// which C library do we need to replace bind in?
#if defined(__LP64__) || defined(_LP64)
#define LIBC_PATH "/lib64/libc.so.6"
#define LIBC_PATH_DEBIAN "/lib/x86_64-linux-gnu/libc.so.6"
#else
#define LIBC_PATH "/lib/libc.so.6"
#define LIBC_PATH_DEBIAN "/lib/i386-linux-gnu/libc.so.6"
#endif

#define CACHE_IP_FILE "/tmp/bind_public.ip"
#define CACHE_IP_LIFETIME 3600


// cache our IP address
static int cache_public_ip( struct sockaddr* addr ) {
   if( addr->sa_family != AF_INET && addr->sa_family != AF_INET6 )
      return 0;
   
   FILE* f = fopen( CACHE_IP_FILE, "w" );
   if( !f ) {
      int errsv = errno;
      fprintf(stderr, "bind_public: cache_public_ip: fopen(%s) errno = %d\n", CACHE_IP_FILE, errsv);
      return errsv;
   }

   switch( addr->sa_family ) {
      case AF_INET:
         // IPv4
         fwrite( &((struct sockaddr_in*)addr)->sin_addr, 1, sizeof( ((struct sockaddr_in*)addr)->sin_addr ), f );
         break;

      case AF_INET6:
         // IPv6
         fwrite( &((struct sockaddr_in6*)addr)->sin6_addr, 1, sizeof( ((struct sockaddr_in6*)addr)->sin6_addr ), f );
         break;

      default:
         break;
   }
   
   fclose( f );
   chmod( CACHE_IP_FILE, 0666 );

   return 0;
}


// get our cached IP address, if it is not stale
// return ESTALE if it is older than CACHE_IP_LIFETIME seconds
static int get_cached_ip( struct sockaddr* addr ) {
   if( addr->sa_family != AF_INET && addr->sa_family != AF_INET6 )
      return EINVAL;
   
   struct stat sb;
   struct timespec ts;
   
   int rc = stat( CACHE_IP_FILE, &sb );
   if( rc != 0 ) {
      int errsv = errno;

      if( errsv != ENOENT ) {
         fprintf(stderr, "bind_public: stat(%s) errno = %d\n", CACHE_IP_FILE, errsv);
      }
      
      return errsv;
   }

   rc = clock_gettime( CLOCK_REALTIME, &ts );
   if( rc != 0 ) {
      int errsv = errno;
      fprintf(stderr, "bind_public: clock_gettime(CLOCK_REALTIME) errno = %d\n", errsv );
      return errsv;
   }

   if( ts.tv_sec > sb.st_mtime + CACHE_IP_LIFETIME ) {
      // stale
      return ESTALE;
   }

   // not stale--read it
   FILE* f = fopen( CACHE_IP_FILE, "r" );
   if( !f ) {
      int errsv = errno;
      fprintf(stderr, "bind_public: fopen(%s) errno = %d\n", CACHE_IP_FILE, errsv );
      return errsv;
   }

   // read it
   
   struct sockaddr_storage buf;
   memset( &buf, 0, sizeof(struct sockaddr_storage) );
   
   size_t nr = 0;
   size_t sz = 0;

   switch( addr->sa_family ) {
      case AF_INET:
         // IPv4
         sz = sizeof( ((struct sockaddr_in*)addr)->sin_addr );
         nr = fread( &((struct sockaddr_in*)addr)->sin_addr, 1, sz, f );
         break;

      case AF_INET6:
         // IPv6
         sz = sizeof( ((struct sockaddr_in6*)addr)->sin6_addr );
         nr = fread( &((struct sockaddr_in6*)addr)->sin6_addr, 1, sz, f );
         break;

      default:
         break;
   }

   if( nr != sz ) {
      // corrupt
      fprintf(stderr, "bind_public: fread(%s) returned %ld != %ld\n", CACHE_IP_FILE, nr, sizeof(struct sockaddr_storage) );
      
      unlink( CACHE_IP_FILE );
      return ENODATA;
   }

   fclose( f );
   
   return 0;
}



// get the node's public IP address, either from cache or DNS
static int get_public_ip( struct sockaddr* addr ) {

   int rc = get_cached_ip( addr );
   if( rc == 0 ) {
      // got it
      return 0;
   }

   struct addrinfo hints;
   memset( &hints, 0, sizeof(hints) );
   hints.ai_family = addr->sa_family;
   hints.ai_flags = AI_CANONNAME;
   hints.ai_protocol = 0;
   hints.ai_canonname = NULL;
   hints.ai_addr = NULL;
   hints.ai_next = NULL;

   rc = 0;

   // get the node hostname
   struct addrinfo *result = NULL;
   char hostname[HOST_NAME_MAX+1];
   gethostname( hostname, HOST_NAME_MAX );

   // get the address information from the hostname
   rc = getaddrinfo( hostname, NULL, &hints, &result );
   if( rc != 0 ) {
      // could not get addr info
      fprintf(stderr, "bind_public: get_public_ip: getaddrinfo: %s\n", gai_strerror( rc ) );
      errno = EINVAL;
      return -errno;
   }

   // NOTE: there should only be one IP address for this node, but it
   // is possible that it can have more.  Here, we just take the first
   // address given.

   switch( addr->sa_family ) {
      case AF_INET:
         // IPv4
         ((struct sockaddr_in*)addr)->sin_addr = ((struct sockaddr_in*)result->ai_addr)->sin_addr;
         break;

      case AF_INET6:
         // IPv6
         ((struct sockaddr_in6*)addr)->sin6_addr = ((struct sockaddr_in6*)result->ai_addr)->sin6_addr;
         break;

      default:
         fprintf(stderr, "bind_public: get_public_ip: unknown socket address family %d\n", addr->sa_family );
         rc = -1;
         break;
   }

   freeaddrinfo( result );

   return rc;
}


// is a particular sockaddr initialized to 0.0.0.0 or ::?
static int is_addr_any( const struct sockaddr* addr ) {
   int ret = 0;

   switch( addr->sa_family ) {
      case AF_INET: {
         // IPv4
         struct sockaddr_in* addr4 = (struct sockaddr_in*)addr;
         if( addr4->sin_addr.s_addr == INADDR_ANY )
            ret = 1;    // this is 0.0.0.0
         break;
      }
      case AF_INET6: {
         // IPv6
         struct sockaddr_in6* addr6 = (struct sockaddr_in6*)addr;
         if( memcmp( &addr6->sin6_addr, &in6addr_any, sizeof(in6addr_any) ) == 0 )
            ret = 1;    // this is ::
         break;
      }
      default:
         // unsupported bind
         fprintf(stderr, "bind_public: is_addr_any: unsupported socket address family %d\n", addr->sa_family );
         ret = -1;
         break;
   }

   return ret;
}


static void print_ip4( uint32_t i ) {
   i = htonl( i );
   printf("%i.%i.%i.%i",
          (i >> 24) & 0xFF,
          (i >> 16) & 0xFF,
          (i >> 8) & 0xFF,
          i & 0xFF);
}

static void print_ip6( uint8_t* bytes ) {
   printf("%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x:%x",
          bytes[0], bytes[1], bytes[2], bytes[3],
          bytes[4], bytes[5], bytes[6],  bytes[7],
          bytes[8],  bytes[9],  bytes[10],  bytes[11],
          bytes[12],  bytes[13],  bytes[14],  bytes[15] );
}

static void debug( const struct sockaddr* before, struct sockaddr* after ) {
   printf("bind_public: ");
   switch( before->sa_family ) {
      case AF_INET:
         print_ip4( ((struct sockaddr_in*)before)->sin_addr.s_addr );
         printf(" --> ");
         print_ip4( ((struct sockaddr_in*)after)->sin_addr.s_addr );
         printf("\n");
         break;
      case AF_INET6:
         print_ip6( ((struct sockaddr_in6*)before)->sin6_addr.s6_addr );
         printf(" --> " );
         print_ip6( ((struct sockaddr_in6*)after)->sin6_addr.s6_addr );
         printf("\n");
         break;
      default:
         printf("UNKNOWN --> UNKNOWN\n");
         break;
   }
   fflush( stdout );
}

// if the caller attempted to bind to 0.0.0.0 or ::, then change it to
// this node's public IP address
int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {

   errno = 0;

   // save the original bind() call
   void *handle = dlopen( LIBC_PATH, RTLD_LAZY );
   if (!handle) {
      handle = dlopen( LIBC_PATH_DEBIAN, RTLD_LAZY );
      if( !handle ) {
         fprintf( stderr, "Error loading libc.so.6\n" );
         fflush( stderr );
         return -1;
      }
   }
   bind_original = dlsym(handle, "bind");
   if( bind_original == NULL ) {
      fprintf( stderr, "Error loading bind symbol\n" );
      fflush( stderr );
      return -1;
   }

   int tried_normal_bind = 0;

   int rc = is_addr_any( addr );

   fprintf( stderr, "bind(%d, %p, %ld)\n", sockfd, addr, addrlen);

   if( rc > 0 ) {

      rc = 0;

      // rewrite this address
      struct sockaddr_storage new_addr;
      memset( &new_addr, 0, sizeof(struct sockaddr_storage));
      memcpy( &new_addr, addr, addrlen );

      rc = get_public_ip( (struct sockaddr*)&new_addr );
      if( rc == -EINVAL ) {
         // this will happen for DHCP, so bind the normal way
         fprintf(stderr, "WARNING: could not get IP address; attempting normal bind.");
         rc = bind_original( sockfd, (struct sockaddr*)&new_addr, addrlen );
         fprintf(stderr, "normal bind rc = %d, errno = %d\n", rc, errno );
         tried_normal_bind = 1;
      }
      else if( rc != 0 ) {
         rc = -1;
      }

      if( rc == 0 && tried_normal_bind == 0 ) {
         debug( addr, (struct sockaddr*)&new_addr );
         rc = bind_original( sockfd, (struct sockaddr*)&new_addr, addrlen );
         fprintf( stderr, "re-addressed bind rc = %d, errno = %d\n", rc, errno);

         // save this result
         if( rc == 0 ) {
            cache_public_ip( (struct sockaddr*)&new_addr);
         }
      }
   }
   else {
      rc = bind_original( sockfd, (struct sockaddr*)addr, addrlen );
      fprintf( stderr, "bind rc = %d, errno = %d\n", rc, errno);
   }
   return rc;
}
