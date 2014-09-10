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
#include <sys/stat.h>
#include <netdb.h>
#include <errno.h>
#include <time.h>

#ifndef HTTP_LOG_SERVER_HOSTNAME
#define HTTP_LOG_SERVER_HOSTNAME "malloy.iplantcollaborative.org"
#endif 

#ifndef HTTP_LOG_SERVER_PORTNUM
#define HTTP_LOG_SERVER_PORTNUM  8090
#endif

#ifndef HTTP_LOG_SERVER_TIMEOUT
#define HTTP_LOG_SERVER_TIMEOUT 30
#endif 

#ifndef HTTP_LOG_SYNC_TIMEOUT
#define HTTP_LOG_SYNC_TIMEOUT 60
#endif

#define xstr(s) str(s)
#define str(s) #s

#define HTTP_LOG_SERVER_PORTNUM_STR     xstr(HTTP_LOG_SERVER_PORTNUM)
#define HTTP_LOG_SERVER_TIMEOUT_STR     xstr(HTTP_LOG_SERVER_TIMEOUT)
#define HTTP_LOG_SYNC_TIMEOUT_STR       xstr(HTTP_LOG_SYNC_TIMEOUT)

#ifdef  __cplusplus
extern "C" {
#endif

int http_sync_all_logs( struct log_context* ctx );
void* http_sync_log_thread( void* arg );

#ifdef  __cplusplus
}
#endif

#endif
