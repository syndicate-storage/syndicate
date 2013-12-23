/*
 * Copyright (c) 2011-2012 Yaroslav Stavnichiy <yarosla@gmail.com>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stddef.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <assert.h>
#include <malloc.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <signal.h>
#include <pthread.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <netdb.h>
#include <errno.h>
#include <math.h>
#include <gcrypt.h>

GCRY_THREAD_OPTION_PTHREAD_IMPL;

//#define WITH_SSL

#ifdef WITH_SSL
#include <gnutls/gnutls.h>
#include <gnutls/x509.h>
#endif

#include <ev.h>

#define VERSION "1.1"

#if (__SIZEOF_POINTER__==8)
typedef uint64_t int_to_ptr;
#else
typedef uint32_t int_to_ptr;
#endif

#define MEM_GUARD 128

struct config {
  int num_connections;
  int num_requests;
  int num_threads;
  int progress_step;
  struct addrinfo *saddr;
  const char* uri_path;
  const char* uri_host;
  const char* ssl_cipher_priority;
  const char* http_method;
  char* request_data;
  int request_length;

#ifdef WITH_SSL
  gnutls_certificate_credentials_t ssl_cred;
  gnutls_priority_t priority_cache;
#endif

  int keep_alive:1;
  int secure:1;

  char _padding1[MEM_GUARD]; // guard from false sharing
  volatile int request_counter;
  char _padding2[MEM_GUARD];
};

static struct config config;

enum nxweb_chunked_decoder_state_code {CDS_CR1=-2, CDS_LF1=-1, CDS_SIZE=0, CDS_LF2, CDS_DATA};

typedef struct nxweb_chunked_decoder_state {
  enum nxweb_chunked_decoder_state_code state;
  unsigned short final_chunk:1;
  unsigned short monitor_only:1;
  int64_t chunk_bytes_left;
} nxweb_chunked_decoder_state;

enum connection_state {C_CONNECTING, C_HANDSHAKING, C_WRITING, C_READING_HEADERS, C_READING_BODY};

#define CONN_BUF_SIZE 32768

typedef struct connection {
  struct ev_loop* loop;
  struct thread_config* tdata;
  int fd;
  ev_io watch_read;
  ev_io watch_write;
  ev_tstamp last_activity;

  nxweb_chunked_decoder_state cdstate;

#ifdef WITH_SSL
  gnutls_session_t session;
#endif

  int write_pos;
  int read_pos;
  int bytes_to_read;
  int bytes_received;
  int alive_count;
  int success_count;
  int time_index;

  int keep_alive:1;
  int chunked:1;
  int done:1;
  int secure:1;

  char buf[CONN_BUF_SIZE];
  char* body_ptr;

  int id;
  enum connection_state state;
} connection;


typedef struct read_time {
   double delta;
   int wrote;
} read_time;

typedef struct thread_config {
  pthread_t tid;
  connection *conns;
  int id;
  int num_conn;
  struct ev_loop* loop;
  ev_tstamp start_time;
  ev_timer watch_heartbeat;

  int shutdown_in_progress;

  int num_success;
  int num_fail;
  long num_bytes_received;
  long num_overhead_received;
  int num_connect;
  ev_tstamp avg_req_time;

  read_time* read_times;
  int num_times;

#ifdef WITH_SSL
  _Bool ssl_identified;
  _Bool ssl_dhe;
  _Bool ssl_ecdh;
  gnutls_kx_algorithm_t ssl_kx;
  gnutls_credentials_type_t ssl_cred;
  int ssl_dh_prime_bits;
  //gnutls_ecc_curve_t ssl_ecc_curve;
  gnutls_protocol_t ssl_protocol;
  gnutls_certificate_type_t ssl_cert_type;
  gnutls_x509_crt_t ssl_cert;
  gnutls_compression_method_t ssl_compression;
  gnutls_cipher_algorithm_t ssl_cipher;
  gnutls_mac_algorithm_t ssl_mac;
#endif
} thread_config;

void nxweb_die(const char* fmt, ...) {
  va_list ap;
  fprintf(stderr, "FATAL: ");
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  exit(EXIT_FAILURE);
}

static inline const char* get_current_time(char* buf, int max_buf_size) {
  time_t t;
  struct tm tm;
  time(&t);
  localtime_r(&t, &tm);
  strftime(buf, max_buf_size, "%F %T", &tm); // %F=%Y-%m-%d %T=%H:%M:%S
  return buf;
}

void nxweb_log_error(const char* fmt, ...) {
  char cur_time[32];
  va_list ap;

  get_current_time(cur_time, sizeof(cur_time));
  flockfile(stderr);
  fprintf(stderr, "%s [%u:%p]: ", cur_time, getpid(), (void*)pthread_self());
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fputc('\n', stderr);
  fflush(stderr);
  funlockfile(stderr);
}

static inline int setup_socket(int fd) {
  int flags=fcntl(fd, F_GETFL);
  if (flags<0) return flags;
  if (fcntl(fd, F_SETFL, flags|=O_NONBLOCK)<0) return -1;

  int nodelay=1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay))) return -1;

//  struct linger linger;
//  linger.l_onoff=1;
//  linger.l_linger=10; // timeout for completing reads/writes
//  setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));

  return 0;
}

static inline void _nxweb_close_good_socket(int fd) {
//  struct linger linger;
//  linger.l_onoff=0; // gracefully shutdown connection
//  linger.l_linger=0;
//  setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));
//  shutdown(fd, SHUT_RDWR);
  close(fd);
}

static inline void _nxweb_close_bad_socket(int fd) {
  struct linger linger;
  linger.l_onoff=1;
  linger.l_linger=0; // timeout for completing writes
  setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));
  close(fd);
}

static inline void sleep_ms(int ms) {
  struct timespec req;
  time_t sec=ms/1000;
  ms%=1000;
  req.tv_sec=sec;
  req.tv_nsec=ms*1000000L;
  while(nanosleep(&req, &req)==-1) continue;
}

/*
static ssize_t gnutls_send( gnutls_transport_ptr_t ptr, const void* buf, size_t buf_len ) {
   ssize_t ret = send( (int)ptr, buf, buf_len, MSG_NOSIGNAL );
   if( ret < 0 ) {
      ret = -errno;
      if( ret == -EAGAIN )
         return -1;
      
      flockfile(stdout);
      printf(" send errno = %ld\n", ret );
      funlockfile(stdout);
      ret = -1;
   }
   return ret;
}
*/
#define CONN_SUCCESS 1
#define CONN_FAILURE 2

static inline void time_start( connection* conn ) {
  struct timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);

  conn->time_index = __sync_add_and_fetch( &conn->tdata->num_times, 1 );

  //printf("[%p] open %d\n", conn->tdata, conn->tdata->num_connect);
  conn->tdata->read_times[ conn->time_index ].delta = (double)ts.tv_sec + ((double)ts.tv_nsec * 1e-9);
  conn->tdata->read_times[ conn->time_index ].wrote = 0;
}


static inline void time_end( connection* conn, int status ) {
   
   if( conn->tdata->read_times[ conn->time_index ].wrote == 0 ) {
      conn->tdata->read_times[ conn->time_index ].wrote = status;

      struct timespec ts;
      clock_gettime (CLOCK_REALTIME, &ts);

      conn->tdata->read_times[ conn->time_index ].delta = (double)ts.tv_sec + ((double)ts.tv_nsec * 1e-9) - conn->tdata->read_times[ conn->time_index ].delta;
      //printf( "[%p] delta[%d] = %ld\n", conn->tdata, conn->time_index, conn->tdata->read_times[ conn->time_index ].delta );
   }
}


static inline void inc_success(connection* conn) {
  conn->success_count++;
  conn->tdata->num_success++;
  conn->tdata->num_bytes_received+=conn->bytes_received;
  conn->tdata->num_overhead_received+=(conn->body_ptr-conn->buf);

  time_end( conn, CONN_SUCCESS );
}

static inline void inc_fail(connection* conn) {
  conn->tdata->num_fail++;

  time_end( conn, CONN_FAILURE );
}

static inline void inc_connect(connection* conn) {
  conn->tdata->num_connect++;
  time_start( conn );
}

enum {ERR_AGAIN=-2, ERR_ERROR=-1, ERR_RDCLOSED=-3};

static inline ssize_t conn_read(connection* conn, void* buf, size_t size) {
#ifdef WITH_SSL
  if (conn->secure) {
    ssize_t ret=gnutls_record_recv(conn->session, buf, size);
    if (ret>0) return ret;
    if (ret==GNUTLS_E_AGAIN) return ERR_AGAIN;
    if (ret==0) return ERR_RDCLOSED;
    return ERR_ERROR;
  }
  else
#endif
  {
    ssize_t ret=read(conn->fd, buf, size);
    if (ret>0) return ret;
    if (ret==0) return ERR_RDCLOSED;
    if (errno==EAGAIN) return ERR_AGAIN;
    return ERR_ERROR;
  }
}

static inline ssize_t conn_write(connection* conn, void* buf, size_t size) {
#ifdef WITH_SSL
  if (conn->secure) {
    ssize_t ret=gnutls_record_send(conn->session, buf, size);
    if (ret>=0) return ret;
    if (ret==GNUTLS_E_AGAIN) return ERR_AGAIN;
    return ERR_ERROR;
  }
  else
#endif
  {
    ssize_t ret=write(conn->fd, buf, size);
    if (ret>=0) return ret;
    if (errno==EAGAIN) return ERR_AGAIN;
    return ERR_ERROR;
  }
}

static inline void conn_close(connection* conn, int good) {
#ifdef WITH_SSL
  if (conn->secure) gnutls_deinit(conn->session);
#endif
  if (good) _nxweb_close_good_socket(conn->fd);
  else _nxweb_close_bad_socket(conn->fd);
}

static int open_socket(connection* conn);
static void rearm_socket(connection* conn);

#ifdef WITH_SSL
static void retrieve_ssl_session_info(connection* conn) {
  if (conn->tdata->ssl_identified) return; // already retrieved
  conn->tdata->ssl_identified=1;
  gnutls_session_t session=conn->session;
  conn->tdata->ssl_kx=gnutls_kx_get(session);
  conn->tdata->ssl_cred=gnutls_auth_get_type(session);
  int dhe=(conn->tdata->ssl_kx==GNUTLS_KX_DHE_RSA || conn->tdata->ssl_kx==GNUTLS_KX_DHE_DSS);
  //int ecdh=(conn->tdata->ssl_kx==GNUTLS_KX_ECDHE_RSA || conn->tdata->ssl_kx==GNUTLS_KX_ECDHE_ECDSA);
  if (dhe) conn->tdata->ssl_dh_prime_bits=gnutls_dh_get_prime_bits(session);
  //if (ecdh) conn->tdata->ssl_ecc_curve=gnutls_ecc_curve_get(session);
  conn->tdata->ssl_dhe=dhe;
  //conn->tdata->ssl_ecdh=ecdh;
  conn->tdata->ssl_protocol=gnutls_protocol_get_version(session);
  conn->tdata->ssl_cert_type=gnutls_certificate_type_get(session);
  if (conn->tdata->ssl_cert_type==GNUTLS_CRT_X509) {
    const gnutls_datum_t *cert_list;
    unsigned int cert_list_size=0;
    cert_list=gnutls_certificate_get_peers(session, &cert_list_size);
    if (cert_list_size>0) {
      gnutls_x509_crt_init(&conn->tdata->ssl_cert);
      gnutls_x509_crt_import(conn->tdata->ssl_cert, &cert_list[0], GNUTLS_X509_FMT_DER);
    }
  }
  conn->tdata->ssl_compression=gnutls_compression_get(session);
  conn->tdata->ssl_cipher=gnutls_cipher_get(session);
  conn->tdata->ssl_mac=gnutls_mac_get(session);
}
#endif // WITH_SSL

static void write_cb(struct ev_loop *loop, ev_io *w, int revents) {
  connection *conn=((connection*)(((char*)w)-offsetof(connection, watch_write)));

  if (conn->state==C_CONNECTING) {
    conn->last_activity=ev_now(loop);
    conn->state=conn->secure? C_HANDSHAKING : C_WRITING;
  }

#ifdef WITH_SSL
  if (conn->state==C_HANDSHAKING) {
    conn->last_activity=ev_now(loop);

    int ret = 0;
    while( 1 ) {
      ret=gnutls_handshake(conn->session);
      if( ret == GNUTLS_E_SUCCESS || ret == GNUTLS_E_AGAIN )
         break;
      if( gnutls_error_is_fatal(ret) )
         break;
    }
    
    if (ret==GNUTLS_E_SUCCESS) {
      retrieve_ssl_session_info(conn);
      conn->state=C_WRITING;
      // fall through to C_WRITING
    }
    else if (ret==GNUTLS_E_AGAIN) {
      if (!gnutls_record_get_direction(conn->session)) {
        ev_io_stop(conn->loop, &conn->watch_write);
        ev_io_start(conn->loop, &conn->watch_read);
      }
      return;
    }
    else {
      nxweb_log_error("gnutls handshake error %d conn=%d", ret, conn->id);
      conn_close(conn, 0);
      inc_fail(conn);
      open_socket(conn);
      return;
    }
  }
#endif // WITH_SSL

  if (conn->state==C_WRITING) {
    int bytes_avail, bytes_sent;
    do {
      bytes_avail=config.request_length-conn->write_pos;
      if (!bytes_avail) {
        conn->state=C_READING_HEADERS;
        conn->read_pos=0;
        ev_io_stop(conn->loop, &conn->watch_write);
        //ev_io_set(&conn->watch_read, conn->fd, EV_READ);
        ev_io_start(conn->loop, &conn->watch_read);
        ev_feed_event(conn->loop, &conn->watch_read, EV_READ);
        return;
      }
      bytes_sent=conn_write(conn, config.request_data+conn->write_pos, bytes_avail);
      if (bytes_sent<0) {
        if (bytes_sent!=ERR_AGAIN) {
          strerror_r(errno, conn->buf, CONN_BUF_SIZE);
          nxweb_log_error("[%d] conn_write() returned %d: %d %s; sent %d of %d bytes total", conn->id, bytes_sent, errno, conn->buf, conn->write_pos, config.request_length);
          conn_close(conn, 0);
          inc_fail(conn);
          open_socket(conn);
          return;
        }
        return;
      }
      if (bytes_sent) conn->last_activity=ev_now(loop);
      conn->write_pos+=bytes_sent;
    } while (bytes_sent==bytes_avail);
    return;
  }
}

static int decode_chunked_stream(nxweb_chunked_decoder_state* decoder_state, char* buf, int* buf_len) {
  char* p=buf;
  char* d=buf;
  char* end=buf+*buf_len;
  char c;
  while (p<end) {
    c=*p;
    switch (decoder_state->state) {
      case CDS_DATA:
        if (end-p>=decoder_state->chunk_bytes_left) {
          p+=decoder_state->chunk_bytes_left;
          decoder_state->chunk_bytes_left=0;
          decoder_state->state=CDS_CR1;
          d=p;
          break;
        }
        else {
          decoder_state->chunk_bytes_left-=(end-p);
          if (!decoder_state->monitor_only) *buf_len=(end-buf);
          return 0;
        }
      case CDS_CR1:
        if (c!='\r') return -1;
        p++;
        decoder_state->state=CDS_LF1;
        break;
      case CDS_LF1:
        if (c!='\n') return -1;
        if (decoder_state->final_chunk) {
          if (!decoder_state->monitor_only) *buf_len=(d-buf);
          return 1;
        }
        p++;
        decoder_state->state=CDS_SIZE;
        break;
      case CDS_SIZE: // read digits until CR2
        if (c=='\r') {
          if (!decoder_state->chunk_bytes_left) {
            // terminator found
            decoder_state->final_chunk=1;
          }
          p++;
          decoder_state->state=CDS_LF2;
        }
        else {
          if (c>='0' && c<='9') c-='0';
          else if (c>='A' && c<='F') c=c-'A'+10;
          else if (c>='a' && c<='f') c=c-'a'+10;
          else return -1;
          decoder_state->chunk_bytes_left=(decoder_state->chunk_bytes_left<<4)+c;
          p++;
        }
        break;
      case CDS_LF2:
        if (c!='\n') return -1;
        p++;
        if (!decoder_state->monitor_only) {
          memmove(d, p, end-p);
          end-=(p-d);
          p=d;
        }
        decoder_state->state=CDS_DATA;
        break;
    }
  }
  if (!decoder_state->monitor_only) *buf_len=(d-buf);
  return 0;
}

static char* find_end_of_http_headers(char* buf, int len, char** start_of_body) {
  if (len<4) return 0;
  char* p;
  for (p=memchr(buf+3, '\n', len-3); p; p=memchr(p+1, '\n', len-(p-buf)-1)) {
    if (*(p-1)=='\n') { *start_of_body=p+1; return p-1; }
    if (*(p-3)=='\r' && *(p-2)=='\n' && *(p-1)=='\r') { *start_of_body=p+1; return p-3; }
  }
  return 0;
}

static int parse_headers(connection* conn) {

  if( strncasecmp( conn->buf, "HTTP/1.1 100 Continue", 21 ) == 0 ) {
     return 100;
  }

  *(conn->body_ptr-1)='\0';
  conn->keep_alive=!strncasecmp(conn->buf, "HTTP/1.1", 8);
  conn->bytes_to_read=-1;


  char *p;
  for (p=strchr(conn->buf, '\n'); p; p=strchr(p, '\n')) {
    p++;
    if (!strncasecmp(p, "Content-Length:", 15)) {
      p+=15;
      while (*p==' ' || *p=='\t') p++;
      conn->bytes_to_read=atoi(p);
    }
    else if (!strncasecmp(p, "Transfer-Encoding:", 18)) {
      p+=18;
      while (*p==' ' || *p=='\t') p++;
      conn->chunked=!strncasecmp(p, "chunked", 7);
    }
    else if (!strncasecmp(p, "Connection:", 11)) {
      p+=11;
      while (*p==' ' || *p=='\t') p++;
      conn->keep_alive=!strncasecmp(p, "keep-alive", 10);
    }
  }

  if (conn->chunked) {
    conn->bytes_to_read=-1;
    memset(&conn->cdstate, 0, sizeof(conn->cdstate));
    conn->cdstate.monitor_only=1;
  }

  conn->bytes_received=conn->read_pos-(conn->body_ptr-conn->buf); // what already read
  return 0;
}

static void read_cb(struct ev_loop *loop, ev_io *w, int revents) {
  connection *conn=((connection*)(((char*)w)-offsetof(connection, watch_read)));

#ifdef WITH_SSL
  if (conn->state==C_HANDSHAKING) {
    conn->last_activity=ev_now(loop);
    int ret = 0;
    while( 1 ) {
      ret=gnutls_handshake(conn->session);
      if( ret == GNUTLS_E_SUCCESS || ret == GNUTLS_E_AGAIN )
         break;
      if( gnutls_error_is_fatal( ret ) )
         break;
    }
    
    if (ret==GNUTLS_E_SUCCESS) {
      retrieve_ssl_session_info(conn);
      conn->state=C_WRITING;
      ev_io_stop(conn->loop, &conn->watch_read);
      ev_io_start(conn->loop, &conn->watch_write);
      return;
    }
    else if (ret==GNUTLS_E_AGAIN) {
      if (gnutls_record_get_direction(conn->session)) {
        ev_io_stop(conn->loop, &conn->watch_read);
        ev_io_start(conn->loop, &conn->watch_write);
      }
      return;
    }
    else {
      nxweb_log_error("gnutls handshake error %d conn=%d", ret, conn->id);
      conn_close(conn, 0);
      inc_fail(conn);
      open_socket(conn);
      return;
    }
  }
#endif // WITH_SSL

  if (conn->state==C_READING_HEADERS) {
    int room_avail, bytes_received;
    do {
      room_avail=CONN_BUF_SIZE-conn->read_pos-1;
      if (!room_avail) {
        // headers too long
        nxweb_log_error("response headers too long");
        conn_close(conn, 0);
        inc_fail(conn);
        open_socket(conn);
        return;
      }
      bytes_received=conn_read(conn, conn->buf+conn->read_pos, room_avail);
      if (bytes_received<=0) {
        if (bytes_received==ERR_AGAIN) return;
        if (bytes_received==ERR_RDCLOSED) {
          conn_close(conn, 0);
          nxweb_log_error("remote close, shutting down %d", conn->fd);
          inc_fail(conn);
          open_socket(conn);
          return;
        }
        strerror_r(errno, conn->buf, CONN_BUF_SIZE);
        nxweb_log_error("headers [%d] conn_read() returned %d error: %d %s", conn->alive_count, bytes_received, errno, conn->buf);
        conn_close(conn, 0);
        inc_fail(conn);
        open_socket(conn);
        return;
      }
      conn->last_activity=ev_now(loop);
      conn->read_pos+=bytes_received;
      //conn->buf[conn->read_pos]='\0';
      if (find_end_of_http_headers(conn->buf, conn->read_pos, &conn->body_ptr)) {
        int header_rc = parse_headers(conn);
        if( header_rc == 100 ) {
           // consume the 100
           find_end_of_http_headers(conn->buf, conn->read_pos, &conn->body_ptr);
           parse_headers(conn);
        }
        if (conn->bytes_to_read<0 && !conn->chunked) {
          nxweb_log_error("response length unknown");
          conn_close(conn, 0);
          inc_fail(conn);
          open_socket(conn);
          return;
        }
        if (!conn->bytes_to_read) { // empty body
          rearm_socket(conn);
          return;
        }

        conn->state=C_READING_BODY;
        if (!conn->chunked) {
          if (conn->bytes_received>=conn->bytes_to_read) {
            // already read all
            rearm_socket(conn);
            return;
          }
        }
        else {
          int r=decode_chunked_stream(&conn->cdstate, conn->body_ptr, &conn->bytes_received);
          if (r<0) {
            nxweb_log_error("chunked encoding error");
            conn_close(conn, 0);
            inc_fail(conn);
            open_socket(conn);
            return;
          }
          else if (r>0) {
            // read all
            rearm_socket(conn);
            return;
          }
        }
        ev_feed_event(conn->loop, &conn->watch_read, EV_READ);
        return;
      }
    } while (bytes_received==room_avail);
    return;
  }

  if (conn->state==C_READING_BODY) {
    int room_avail, bytes_received, bytes_received2, r;
    conn->last_activity=ev_now(loop);
    do {
      room_avail=CONN_BUF_SIZE;
      if (conn->bytes_to_read>0) {
        int bytes_left=conn->bytes_to_read - conn->bytes_received;
        if (bytes_left<room_avail) room_avail=bytes_left;
      }
      bytes_received=conn_read(conn, conn->buf, room_avail);
      if (bytes_received<=0) {
        if (bytes_received==ERR_AGAIN) return;
        if (bytes_received==ERR_RDCLOSED) {
          nxweb_log_error("body [%d] read connection closed", conn->alive_count);
          conn_close(conn, 0);
          inc_fail(conn);
          open_socket(conn);
          return;
        }
        strerror_r(errno, conn->buf, CONN_BUF_SIZE);
        nxweb_log_error("body [%d] conn_read() returned %d error: %d %s", conn->alive_count, bytes_received, errno, conn->buf);
        conn_close(conn, 0);
        inc_fail(conn);
        open_socket(conn);
        return;
      }

      if (!conn->chunked) {
        conn->bytes_received+=bytes_received;
        if (conn->bytes_received>=conn->bytes_to_read) {
          // read all
          rearm_socket(conn);
          return;
        }
      }
      else {
        bytes_received2=bytes_received;
        r=decode_chunked_stream(&conn->cdstate, conn->buf, &bytes_received2);
        if (r<0) {
          nxweb_log_error("chunked encoding error after %d bytes received", conn->bytes_received);
          conn_close(conn, 0);
          inc_fail(conn);
          open_socket(conn);
          return;
        }
        else if (r>0) {
          conn->bytes_received+=bytes_received2;
          // read all
          rearm_socket(conn);
          return;
        }
      }

    } while (bytes_received==room_avail);
    return;
  }
}

static void shutdown_thread(thread_config* tdata) {
  int i;
  connection* conn;
  ev_tstamp now=ev_now(tdata->loop);
  ev_tstamp time_limit=15.;
  
  //fprintf(stderr, "[%.6lf]", time_limit);
  for (i=0; i<tdata->num_conn; i++) {
    conn=&tdata->conns[i];
    if (!conn->done) {
      if (ev_is_active(&conn->watch_read) || ev_is_active(&conn->watch_write)) {
        if ( 0 && (now - conn->last_activity) > time_limit) {
          // kill this connection
          if (ev_is_active(&conn->watch_write)) ev_io_stop(conn->loop, &conn->watch_write);
          if (ev_is_active(&conn->watch_read)) ev_io_stop(conn->loop, &conn->watch_read);
          conn_close(conn, 0);
          nxweb_log_error("forcibly shutting down [%p] : %d : %d", tdata, conn->id, conn->fd);
          inc_fail(conn);
          conn->done=1;
          //fprintf(stderr, "*");
        }
        else {
          // don't kill this yet, but wake it up
          if (ev_is_active(&conn->watch_read)) {
            ev_feed_event(tdata->loop, &conn->watch_read, EV_READ);
          }
          if (ev_is_active(&conn->watch_write)) {
            ev_feed_event(tdata->loop, &conn->watch_write, EV_WRITE);
          }
          //fprintf(stderr, ".");
        }
      }
    }
  }
}

static int more_requests_to_run() {
  int rc=__sync_add_and_fetch(&config.request_counter, 1);
  if (rc>config.num_requests) {
    return 0;
  }
  if (config.progress_step>=10 && (rc%config.progress_step==0 || rc==config.num_requests)) {
    printf("%d requests launched\n", rc);
  }
  return rc;
}

static void heartbeat_cb(struct ev_loop *loop, ev_timer *w, int revents) {
  if (config.request_counter>config.num_requests) {
    thread_config *tdata=((thread_config*)(((char*)w)-offsetof(thread_config, watch_heartbeat)));
    if (!tdata->shutdown_in_progress) {
      ev_tstamp now=ev_now(tdata->loop);
      tdata->avg_req_time=tdata->num_success? (now-tdata->start_time) * tdata->num_conn / tdata->num_success : 0.1;
      if (tdata->avg_req_time>1.) tdata->avg_req_time=1.;
      tdata->shutdown_in_progress=1;
    }
    shutdown_thread(tdata);
  }
}

static void rearm_socket(connection* conn) {
  if (ev_is_active(&conn->watch_write)) ev_io_stop(conn->loop, &conn->watch_write);
  if (ev_is_active(&conn->watch_read)) ev_io_stop(conn->loop, &conn->watch_read);

  inc_success(conn);

  if (!config.keep_alive || !conn->keep_alive) {
    conn_close(conn, 1);
    open_socket(conn);
  }
  else {
    int nc = more_requests_to_run();
    if (!nc) {
      conn_close(conn, 1);
      conn->done=1;
      ev_feed_event(conn->tdata->loop, &conn->tdata->watch_heartbeat, EV_TIMER);
      return;
    }
    conn->alive_count++;
    conn->state=C_WRITING;
    conn->write_pos=0;
    conn->id = nc;
    time_start(conn);
    ev_io_start(conn->loop, &conn->watch_write);
    ev_feed_event(conn->loop, &conn->watch_write, EV_WRITE);
  }
}

static int open_socket(connection* conn) {

  if (ev_is_active(&conn->watch_write)) ev_io_stop(conn->loop, &conn->watch_write);
  if (ev_is_active(&conn->watch_read)) ev_io_stop(conn->loop, &conn->watch_read);

  int nc = more_requests_to_run();
  if (!nc) {
    conn->done=1;
    ev_feed_event(conn->tdata->loop, &conn->tdata->watch_heartbeat, EV_TIMER);
    return 1;
  }

  inc_connect(conn);
  conn->id = nc;
  conn->fd=socket(config.saddr->ai_family, config.saddr->ai_socktype, config.saddr->ai_protocol);
  if (conn->fd==-1) {
    strerror_r(errno, conn->buf, CONN_BUF_SIZE);
    nxweb_log_error("can't open socket [%d] %s", errno, conn->buf);
    return -1;
  }
  if (setup_socket(conn->fd)) {
    nxweb_log_error("can't setup socket");
    return -1;
  }
  if (connect(conn->fd, config.saddr->ai_addr, config.saddr->ai_addrlen)) {
    if (errno!=EINPROGRESS && errno!=EALREADY && errno!=EISCONN) {
      nxweb_log_error("can't connect %d", errno);
      return -1;
    }
  }

#ifdef WITH_SSL
  if (config.secure) {
    gnutls_init(&conn->session, GNUTLS_CLIENT);
    gnutls_server_name_set(conn->session, GNUTLS_NAME_DNS, config.uri_host, strlen(config.uri_host));
    gnutls_priority_set(conn->session, config.priority_cache);
    gnutls_credentials_set(conn->session, GNUTLS_CRD_CERTIFICATE, config.ssl_cred);
    gnutls_transport_set_ptr(conn->session, (gnutls_transport_ptr_t)(int_to_ptr)conn->fd);
    //gnutls_transport_set_push_function(conn->session, gnutls_send );
  }
#endif // WITH_SSL

  conn->state=C_CONNECTING;
  conn->write_pos=0;
  conn->alive_count=0;
  conn->done=0;
  ev_io_set(&conn->watch_write, conn->fd, EV_WRITE);
  ev_io_set(&conn->watch_read, conn->fd, EV_READ);
  ev_io_start(conn->loop, &conn->watch_write);
  ev_feed_event(conn->loop, &conn->watch_write, EV_WRITE);
  return 0;
}

static void* thread_main(void* pdata) {

  thread_config* tdata=(thread_config*)pdata;

//  tdata->loop=ev_loop_new(0);
//
//  int i;
//  connection* conn;
//  for (i=0; i<tdata->num_conn; i++) {
//    conn=&tdata->conns[i];
//    conn->tdata=tdata;
//    conn->loop=tdata->loop;
//    ev_io_init(&conn->watch_write, write_cb, -1, EV_WRITE);
//    ev_io_init(&conn->watch_read, read_cb, -1, EV_READ);
//    if (open_socket(conn)) return 0;
//  }

  ev_timer_init(&tdata->watch_heartbeat, heartbeat_cb, 0.1, 0.1);
  ev_timer_start(tdata->loop, &tdata->watch_heartbeat);
  ev_unref(tdata->loop); // don't keep loop running just for heartbeat
  ev_run(tdata->loop, 0);

  ev_loop_destroy(tdata->loop);

  if (config.num_threads>1) {
    printf("thread %d: %d connect, %d requests, %d success, %d fail, %ld bytes, %ld overhead\n",
         tdata->id, tdata->num_connect, tdata->num_success+tdata->num_fail,
         tdata->num_success, tdata->num_fail, tdata->num_bytes_received,
         tdata->num_overhead_received);
  }

  return 0;
}


static int resolve_host(struct addrinfo** saddr, const char *host_and_port) {
  char* host=strdup(host_and_port);
  char* port=strchr(host, ':');
  if (port) *port++='\0';
  else port=config.secure? "443":"80";

	struct addrinfo hints, *res, *res_first, *res_last;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family=PF_UNSPEC;
	hints.ai_socktype=SOCK_STREAM;

	if (getaddrinfo(host, port, &hints, &res_first)) goto ERR1;

	// search for an ipv4 address, no ipv6 yet
	res_last=0;
	for (res=res_first; res; res=res->ai_next) {
		if (res->ai_family==AF_INET) break;
		res_last=res;
	}

	if (!res) goto ERR2;
	if (res!=res_first) {
		// unlink from list and free rest
		res_last->ai_next = res->ai_next;
		freeaddrinfo(res_first);
		res->ai_next=0;
	}

  free(host);
  *saddr=res;
	return 0;

ERR2:
	freeaddrinfo(res_first);
ERR1:
  free(host);
  return -1;
}


static char b64_encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                                    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                                    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                                    'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                                    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
                                    'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                                    'w', 'x', 'y', 'z', '0', '1', '2', '3',
                                    '4', '5', '6', '7', '8', '9', '+', '/'};
static char *b64_decoding_table = NULL;
static int mod_table[] = {0, 2, 1};


void build_decoding_table() {

    b64_decoding_table = malloc(256);

    int i;
    for (i = 0; i < 0x40; i++)
        b64_decoding_table[(int)b64_encoding_table[i]] = i;
}


char *base64_encode(const char *data,
                    size_t input_length,
                    size_t *output_length) {

    *output_length = (size_t) (4.0 * ceil((double) input_length / 3.0));

    char *encoded_data = malloc(*output_length);
    if (encoded_data == NULL) return NULL;

    int i, j;
    for (i = 0, j = 0; i < input_length;) {

        uint32_t octet_a = i < input_length ? data[i++] : 0;
        uint32_t octet_b = i < input_length ? data[i++] : 0;
        uint32_t octet_c = i < input_length ? data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = b64_encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = b64_encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = b64_encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = b64_encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (i = 0; i < mod_table[input_length % 3]; i++)
        encoded_data[*output_length - 1 - i] = '=';

    return encoded_data;
}


char *base64_decode(const char *data,
                    size_t input_length,
                    size_t *output_length) {

    if (b64_decoding_table == NULL) build_decoding_table();

    if (input_length % 4 != 0) return NULL;

    *output_length = input_length / 4 * 3;
    if (data[input_length - 1] == '=') (*output_length)--;
    if (data[input_length - 2] == '=') (*output_length)--;

    char *decoded_data = malloc(*output_length);
    if (decoded_data == NULL) return NULL;

    int i, j;
    for (i = 0, j = 0; i < input_length;) {

        uint32_t sextet_a = data[i] == '=' ? 0 & i++ : b64_decoding_table[(int)data[i++]];
        uint32_t sextet_b = data[i] == '=' ? 0 & i++ : b64_decoding_table[(int)data[i++]];
        uint32_t sextet_c = data[i] == '=' ? 0 & i++ : b64_decoding_table[(int)data[i++]];
        uint32_t sextet_d = data[i] == '=' ? 0 & i++ : b64_decoding_table[(int)data[i++]];

        uint32_t triple = (sextet_a << 3 * 6)
                        + (sextet_b << 2 * 6)
                        + (sextet_c << 1 * 6)
                        + (sextet_d << 0 * 6);

        if (j < *output_length) decoded_data[j++] = (triple >> 2 * 8) & 0xFF;
        if (j < *output_length) decoded_data[j++] = (triple >> 1 * 8) & 0xFF;
        if (j < *output_length) decoded_data[j++] = (triple >> 0 * 8) & 0xFF;
    }

    return decoded_data;
}


void base64_cleanup() {
    free(b64_decoding_table);
}


static void show_help(void) {
	printf( "httpress <options> <url>\n"
          "  -n num         number of requests     (default: 1)\n"
          "  -t num         number of threads      (default: 1)\n"
          "  -c num         concurrent connections (default: 1)\n"
          "  -k             keep alive             (default: no)\n"
          "  -z pri         GNUTLS cipher priority (default: NORMAL)\n"
          "  -M method      HTTP method            (default: GET)\n"
          "  -F file        POST/PUT file path\n"
          "  -h             show this help\n"
          "  -T type        Content-Type\n"
          "  -A user:pass   Authorization\n"
          "  -R file        File containing raw information to send\n"
          //"  -v       show version\n"
          "\n"
          "example: httpress -n 10000 -c 100 -t 4 -k http://localhost:8080/index.html\n\n");
}

static char host_buf[1024];


int time_compar( const void* a1, const void* a2 ) {
   if( ((read_time*)a1)->delta < ((read_time*)a2)->delta )
      return -1;
   if( ((read_time*)a1)->delta > ((read_time*)a2)->delta )
      return 1;
   return 0;
}

static int parse_uri(const char* uri) {
  if (!strncmp(uri, "http://", 7)) uri+=7;
#ifdef WITH_SSL
  else if (!strncmp(uri, "https://", 8)) { uri+=8; config.secure=1; }
#endif
  else return -1;

  const char* p=strchr(uri, '/');
  if (!p) {
    config.uri_host=uri;
    config.uri_path="/";
    return 0;
  }
  if ((p-uri)>sizeof(host_buf)-1) return -1;
  strncpy(host_buf, uri, (p-uri));
  host_buf[(p-uri)]='\0';
  config.uri_host=host_buf;
  config.uri_path=p;
  return 0;
}

int main(int argc, char* argv[]) {
  gcry_control (GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread); 
  gcry_check_version("1"); 
  gnutls_global_init(); 

  config.num_connections=1;
  config.num_requests=1;
  config.num_threads=1;
  config.keep_alive=0;
  config.uri_path=0;
  config.uri_host=0;
  config.http_method="GET";
  config.request_counter=0;
  config.ssl_cipher_priority="NORMAL"; // NORMAL:-CIPHER-ALL:+AES-256-CBC:-VERS-TLS-ALL:+VERS-TLS1.0:-KX-ALL:+DHE-RSA

  char const* upload_file = NULL;
  char const* content_type = NULL;
  char const* userpass = NULL;
  char const* raw_file = NULL;

  int c;
  while ((c=getopt(argc, argv, ":hvkn:t:c:z:M:F:T:A:R:"))!=-1) {
    switch (c) {
      case 'h':
        show_help();
        return 0;
      case 'v':
        printf("version:    " VERSION "\n");
        printf("build-date: " __DATE__ " " __TIME__ "\n\n");
        return 0;
      case 'k':
        config.keep_alive=1;
        break;
      case 'n':
        config.num_requests=atoi(optarg);
        break;
      case 't':
        config.num_threads=atoi(optarg);
        break;
      case 'c':
        config.num_connections=atoi(optarg);
        break;
      case 'z':
        config.ssl_cipher_priority=optarg;
        break;
      case 'M':
        config.http_method=optarg;
        break;
      case 'F':
        upload_file=optarg;
        break;
      case 'T':
        content_type=optarg;
        break;
      case 'A':
        userpass=optarg;
        break;
      case 'R':
        raw_file=optarg;
        break;
      case '?':
        fprintf(stderr, "unkown option: -%c\n\n", optopt);
        show_help();
        return EXIT_FAILURE;
    }
  }

  if ((argc-optind)<1) {
    fprintf(stderr, "missing url argument\n\n");
    show_help();
    return EXIT_FAILURE;
  }
  else if ((argc-optind)>1) {
    fprintf(stderr, "too many arguments\n\n");
    show_help();
    return EXIT_FAILURE;
  }

  if (config.num_requests<1 || config.num_requests>1000000000) nxweb_die("wrong number of requests");
  if (config.num_connections<1 || config.num_connections>1000000 || config.num_connections>config.num_requests) nxweb_die("wrong number of connections");
  if (config.num_threads<1 || config.num_threads>100000 || config.num_threads>config.num_connections) nxweb_die("wrong number of threads");

  config.progress_step=config.num_requests/4;
  if (config.progress_step>50000) config.progress_step=50000;

  if (parse_uri(argv[optind])) nxweb_die("can't parse url: %s", argv[optind]);


#ifdef WITH_SSL
  if (config.secure) {
    gnutls_global_init();
    gnutls_certificate_allocate_credentials(&config.ssl_cred);
    int ret=gnutls_priority_init(&config.priority_cache, config.ssl_cipher_priority, 0);
    if (ret) {
      fprintf(stderr, "invalid priority string: %s\n\n", config.ssl_cipher_priority);
      return EXIT_FAILURE;
    }
  }
#endif // WITH_SSL


  // Block signals for all threads
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGPIPE);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGQUIT);
  sigaddset(&set, SIGHUP);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL)) {
    nxweb_log_error("can't set pthread_sigmask");
    exit(EXIT_FAILURE);
  }

  if (resolve_host(&config.saddr, config.uri_host)) {
    nxweb_log_error("can't resolve host %s", config.uri_host);
    exit(EXIT_FAILURE);
  }

  char content_type_header[1024];
  if( content_type ) {
     sprintf( content_type_header, "Content-Type: %s\r\n", content_type );
  }
  else {
     content_type_header[0] = '\0';
  }

  char authorization_header[4096];
  if( userpass ) {
     size_t userpass_b64_len = 0;
     char* userpass_b64 = base64_encode( userpass, strlen(userpass), &userpass_b64_len );
     
     strcpy( authorization_header, "Authorization: Basic " );
     strncat( authorization_header, userpass_b64, userpass_b64_len );
     strcat( authorization_header, "\r\n" );

     free( userpass_b64 );
  }
  else {
     authorization_header[0] = '\0';
  }

  if( upload_file ) {
      FILE* f = fopen( upload_file, "r" );
      if( !f ) {
         int errsv = errno;
         nxweb_log_error("could not open %s, errno = %d\n", upload_file, errsv );
         exit(1);
      }

      struct stat sb;
      int rc = fstat( fileno(f), &sb );
      if( rc != 0 ) {
         int errsv = errno;
         nxweb_log_error("could not stat %s, errno = %d\n", upload_file, errsv );
         exit(1);
      }

      config.request_data = calloc( 8192 + sb.st_size, 1 );

      int off = sprintf( config.request_data,
                        "%s %s HTTP/1.1\r\n"
                        "Host: %s\r\n"
                        "Connection: %s\r\n"
                        "Content-Length: %lu\r\n"
                        "%s"
                        "%s"
                        "\r\n",
                        config.http_method, config.uri_path, config.uri_host, (config.keep_alive?"keep-alive":"close"), sb.st_size, content_type_header, authorization_header
                );
      

      fread( config.request_data + off, 1, sb.st_size, f );
      config.request_length = off + sb.st_size;
      fclose( f );
  }
  else if( raw_file ) {
      FILE* raw_fh = fopen( raw_file, "r" );
      if( !raw_fh ) {
         int errsv = errno;
         nxweb_log_error("Could not open %s, errno = %d\n", raw_file, errsv );
         exit(1);
      }

      struct stat sb;
      int rc = fstat( fileno(raw_fh), &sb );
      if( rc != 0 ) {
         int errsv = errno;
         nxweb_log_error("Could not stat %s, errno = %d\n", raw_file, errsv );
         exit(1);
      }

      config.request_data = calloc( sb.st_size, 1 );
      fread( config.request_data, 1, sb.st_size, raw_fh );
      config.request_length = sb.st_size;
      fclose( raw_fh );
  }
  else {
     config.request_data = calloc( 8192, 1 );

     snprintf(config.request_data, 8192,
               "%s %s HTTP/1.1\r\n"
               "Host: %s\r\n"
               "Connection: %s\r\n"
               "%s"
               "%s"
               "\r\n",
               config.http_method, config.uri_path, config.uri_host, config.keep_alive?"keep-alive":"close", content_type_header, authorization_header
               );
     config.request_length=strlen(config.request_data);
  }

  printf("<<<<<<<<<<<<<< %d <<<<<<<<<<<<<<\n%s\n<<<<<<<<<<<<<< %d <<<<<<<<<<<<<<\n", config.request_length, config.request_data, config.request_length );

  thread_config** threads=calloc(config.num_threads, sizeof(thread_config*));
  if (!threads) nxweb_die("can't allocate thread pool");

  ev_tstamp ts_start=ev_time();
  int i, j;
  int conns_allocated=0;
  thread_config* tdata;
  for (i=0; i<config.num_threads; i++) {
    threads[i]=
    tdata=memalign(MEM_GUARD, sizeof(thread_config)+MEM_GUARD);
    if (!tdata) nxweb_die("can't allocate thread data");
    memset(tdata, 0, sizeof(thread_config));
    tdata->id=i+1;
    tdata->start_time=ts_start;
    tdata->num_conn=(config.num_connections-conns_allocated)/(config.num_threads-i);
    conns_allocated+=tdata->num_conn;
    tdata->conns=memalign(MEM_GUARD, tdata->num_conn*sizeof(connection)+MEM_GUARD);
    if (!tdata->conns) nxweb_die("can't allocate thread connection pool");
    memset(tdata->conns, 0, tdata->num_conn*sizeof(connection));

    tdata->read_times=memalign(MEM_GUARD, config.num_requests*sizeof(read_time)+MEM_GUARD);
    if (!tdata->read_times) nxweb_die("can't allocate thread read times");
    memset(tdata->read_times, 0, config.num_requests*sizeof(read_time));
    
    tdata->loop=ev_loop_new(0);

    connection* conn;
    for (j=0; j<tdata->num_conn; j++) {
      conn=&tdata->conns[j];
      conn->tdata=tdata;
      conn->loop=tdata->loop;
      conn->secure=config.secure;
      ev_io_init(&conn->watch_write, write_cb, -1, EV_WRITE);
      ev_io_init(&conn->watch_read, read_cb, -1, EV_READ);
      open_socket(conn);
    }

    pthread_create(&tdata->tid, 0, thread_main, tdata);
    //sleep_ms(10);
  }

  // Unblock signals for the main thread;
  // other threads have inherited sigmask we set earlier
  sigdelset(&set, SIGPIPE); // except SIGPIPE
  if (pthread_sigmask(SIG_UNBLOCK, &set, NULL)) {
    nxweb_log_error("can't unset pthread_sigmask");
    exit(EXIT_FAILURE);
  }

  int total_success=0;
  int total_fail=0;
  long total_bytes=0;
  long total_overhead=0;
  int total_connect=0;

  for (i=0; i<config.num_threads; i++) {
    tdata=threads[i];
    pthread_join(threads[i]->tid, 0);
    total_success+=tdata->num_success;
    //total_success+=tdata->num_connect;
    total_fail+=tdata->num_fail;
    total_bytes+=tdata->num_bytes_received;
    total_overhead+=tdata->num_overhead_received;
    total_connect+=tdata->num_connect;
  }

  int real_concurrency=0;
  int real_concurrency1=0;
  int real_concurrency1_threshold=config.num_requests/config.num_connections/10;
  if (real_concurrency1_threshold<2) real_concurrency1_threshold=2;
  for (i=0; i<config.num_threads; i++) {
    tdata=threads[i];
    for (j=0; j<tdata->num_conn; j++) {
      connection* conn=&tdata->conns[j];
      if (conn->success_count) real_concurrency++;
      if (conn->success_count>=real_concurrency1_threshold) real_concurrency1++;
    }
  }

  ev_tstamp ts_end=ev_time();
  if (ts_end<=ts_start) ts_end=ts_start+0.00001;
  ev_tstamp duration=ts_end-ts_start;
  int sec=duration;
  duration=(duration-sec)*1000;
  int millisec=duration;
  duration=(duration-millisec)*1000;
  //int microsec=duration;
  int rps=total_success/(ts_end-ts_start);
  int kbps=(total_bytes+total_overhead) / (ts_end-ts_start) / 1024;
  ev_tstamp avg_req_time=total_success? (ts_end-ts_start) * config.num_connections / total_success : 0;

#ifdef WITH_SSL
  if (config.secure) {
    for (i=0; i<config.num_threads; i++) {
      tdata=threads[i];
      if (tdata->ssl_identified) {
        printf("\nSSL INFO: %s\n", gnutls_cipher_suite_get_name(tdata->ssl_kx, tdata->ssl_cipher, tdata->ssl_mac));
        printf ("- Protocol: %s\n", gnutls_protocol_get_name(tdata->ssl_protocol));
        printf ("- Key Exchange: %s\n", gnutls_kx_get_name(tdata->ssl_kx));
        /*
        if (tdata->ssl_ecdh) printf ("- Ephemeral ECDH using curve %s\n",
                  gnutls_ecc_curve_get_name(tdata->ssl_ecc_curve));
        */
        if (tdata->ssl_dhe) printf ("- Ephemeral DH using prime of %d bits\n",
                  tdata->ssl_dh_prime_bits);
        printf ("- Cipher: %s\n", gnutls_cipher_get_name(tdata->ssl_cipher));
        printf ("- MAC: %s\n", gnutls_mac_get_name(tdata->ssl_mac));
        printf ("- Compression: %s\n", gnutls_compression_get_name(tdata->ssl_compression));
        printf ("- Certificate Type: %s\n", gnutls_certificate_type_get_name(tdata->ssl_cert_type));
        if (tdata->ssl_cert) {
          gnutls_datum_t cinfo;
          if (!gnutls_x509_crt_print(tdata->ssl_cert, GNUTLS_CRT_PRINT_ONELINE, &cinfo)) {
            printf ("- Certificate Info: %s\n", cinfo.data);
            gnutls_free(cinfo.data);
          }
        }
        break;
      }
    }
  }
#endif // WITH_SSL

  printf("\nTOTALS:  %d connect, %d requests, %d success, %d fail, %d (%d) real concurrency\n",
         total_connect, total_success+total_fail, total_success, total_fail, real_concurrency, real_concurrency1);
  printf("TRAFFIC: %ld avg bytes, %ld avg overhead, %ld bytes, %ld overhead\n",
         total_success?total_bytes/total_success:0L, total_success?total_overhead/total_success:0L, total_bytes, total_overhead);
  printf("TIMING:  %d.%03d seconds, %d rps, %d kbps, %.1f ms avg req time\n",
         sec, millisec, /*microsec,*/ rps, kbps, (float)(avg_req_time*1000));

  size_t num_xfer = total_success + total_fail;
  printf(" total transfers = %ld\n", num_xfer );
  
  read_time* all_times = calloc( num_xfer * sizeof(read_time), 1 );
  int k = 0;
  for( i = 0; i < config.num_threads; i++ ) {
     for( j = 0; j < threads[i]->num_times; j++ ) {
        all_times[k] = threads[i]->read_times[j];
        k++;
     }
  }

  double total = 0;
  qsort( all_times, num_xfer, sizeof(read_time), time_compar );

  FILE* f = fopen( "all-time.txt", "w" );
  for( i = 0; i < num_xfer; i++ ) {
     total += all_times[i].delta;

     char c = 'U';
     if( all_times[i].wrote == CONN_SUCCESS )
        c = 'S';
     else if( all_times[i].wrote == CONN_FAILURE )
        c = 'F';
     
     fprintf( f, "%c %lf\n", c, all_times[i].delta );
  }
  fclose( f );
  
  total /= num_xfer;

  printf("All-time average (ms):   %lf\n", total * 1000 );
  printf("All-time median (ms):    %lf\n", all_times[ num_xfer / 2 ].delta * 1000 );
  printf("All-time 90th time (ms): %lf\n", all_times[ num_xfer * 9 / 10 ].delta * 1000 );
  printf("All-time 99th time (ms): %lf\n", all_times[ num_xfer * 99 / 100 ].delta * 1000 );

  /*
  for( i = 0; i < config.num_threads; i++ ) {
     total = 0;

     qsort( threads[i]->read_times, threads[i]->num_connect, sizeof(read_time), time_compar );

     char name[20];
     sprintf(name, "thread-%d.txt", i );
     
     FILE* f = fopen( name, "w" );
     for( j = 0; j < threads[i]->num_connect; j++ ) {
         fprintf( f, "%d %c %lf\n", i, (threads[i]->read_times[j].status == 1 ? 'S' : 'F'), threads[i]->read_times[j].delta );
     }
     fclose( f );
     
     for( j = 0; j < threads[i]->num_connect; j++ ) {
         total += threads[i]->read_times[j].delta;
     }
     total /= threads[i]->num_connect;
     printf( "Thread %d average time (ms): %lf\n", i, total * 1000. );
     printf( "Thread %d median time (ms):  %lf\n", i, threads[i]->read_times[ threads[i]->num_connect/2 ].delta * 1000. );
     printf( "Thread %d 90th time (ms):    %lf\n", i, threads[i]->read_times[ (threads[i]->num_connect*9) / 10 ].delta * 1000.0 );
     printf( "Thread %d 99th time (ms):    %lf\n", i, threads[i]->read_times[ (threads[i]->num_connect*99) / 100 ].delta * 1000.0 );
     printf("\n");
  }
  */
     
  freeaddrinfo(config.saddr);
  for (i=0; i<config.num_threads; i++) {
    tdata=threads[i];
    free(tdata->conns);
#ifdef WITH_SSL
    if (tdata->ssl_cert) gnutls_x509_crt_deinit(tdata->ssl_cert);
#endif // WITH_SSL
    free(tdata);
  }
  free(threads);

#ifdef WITH_SSL
  if (config.secure) {
    gnutls_certificate_free_credentials(config.ssl_cred);
    gnutls_priority_deinit(config.priority_cache);
    gnutls_global_deinit();
  }
#endif // WITH_SSL

  return EXIT_SUCCESS;
}
