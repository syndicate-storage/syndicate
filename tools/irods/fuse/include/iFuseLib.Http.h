#ifndef _I_FUSELIB_HTTP_H_
#define _I_FUSELIB_HTTP_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

#define HTTP_LOG_SERVER_HOSTNAME "vm64-125.iplantcollaborative.org"
#define HTTP_LOG_SERVER_PORTNUM  8090

int http_connect( char const* hostname, int portnum );
int http_upload( int socket_fd, int file_fd, size_t num_bytes );

#endif
