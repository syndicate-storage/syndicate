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
/*
 * Utility functions (debugging, etc)
 */
 
#include "libsyndicate/util.h"
#include "libsyndicate/libsyndicate.h"

int _DEBUG = 1;

int _DEBUG_MESSAGES = 0;
int _ERROR_MESSAGES = 1;


void set_debug_level( int d ) {
   _DEBUG_MESSAGES = d;
}

void set_error_level( int e ) {
   _ERROR_MESSAGES = e;
}

int get_debug_level() {
   return _DEBUG_MESSAGES;
}

int get_error_level() {
   return _ERROR_MESSAGES;
}

/* Converts a hex character to its integer value */
char from_hex(char ch) {
  return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10;
}

/* Converts an integer value to its hex character*/
char to_hex(char code) {
  static char hex[] = "0123456789abcdef";
  return hex[code & 15];
}


// concatenate two paths
char* fullpath( char* root, const char* path ) {
   char delim = 0;
   
   int len = strlen(path) + strlen(root) + 1;
   if( root[strlen(root)-1] != '/' ) {
      len++;
      delim = '/';
   }
   
   char* ret = (char*)malloc( len );
   
   if( ret == NULL )
      return NULL;
   
   bzero(ret, len);
      
   strcpy( ret, root );
   if( delim != 0 ) {
      ret[strlen(ret)] = '/';
   }
   strcat( ret, path );
   return ret;
}

/* Allocate a path from the given path, with a '/' added to the end */
char* dir_path( const char* path ) {
   int len = strlen(path);
   char* ret = NULL;
   
   if( path[len-1] != '/' ) {
      ret = (char*)malloc( len + 2 );
      if( ret == NULL )
         return NULL;
         
      sprintf( ret, "%s/", path );
   }
   else {
      ret = (char*)malloc( len + 1 );
      if( ret == NULL )
         return NULL;
         
      strcpy( ret, path );
   }
   
   return ret;
}


/*
 * Get the current time in seconds since the epoch, local time
 */
int64_t currentTimeSeconds() {
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   int64_t ts_sec = (int64_t)ts.tv_sec;
   return ts_sec;
}


// get the current time in milliseconds
int64_t currentTimeMillis() {
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   int64_t ts_sec = (int64_t)ts.tv_sec;
   int64_t ts_nsec = (int64_t)ts.tv_nsec;
   return (ts_sec * 1000) + (ts_nsec / 1000000);
}

// get the current time in microseconds
int64_t currentTimeMicros() {
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   int64_t ts_sec = (int64_t)ts.tv_sec;
   int64_t ts_nsec = (int64_t)ts.tv_nsec;
   return (ts_sec * 1000000) + (ts_nsec / 1000);
}

double currentTimeMono() {
   struct timespec ts;
   clock_gettime( CLOCK_MONOTONIC, &ts );
   return (ts.tv_sec * 1e9) + (double)ts.tv_nsec;
}

/*
 * Get the user's umask
 */
mode_t get_umask() {
  mode_t mask = umask(0);
  umask(mask);
  return mask;
}


// calculate the sha-256 hash of something.
// caller must free the hash buffer.
unsigned char* sha256_hash_data( char const* input, size_t len ) {
   unsigned char* obuf = (unsigned char*)calloc( SHA256_DIGEST_LENGTH, 1 );
   SHA256( (unsigned char*)input, len, obuf );
   return obuf;
}

size_t sha256_len(void) {
   return SHA256_DIGEST_LENGTH;
}

// calculate the sha-256 hash of a string
unsigned char* sha256_hash( char const* input ) {
   return sha256_hash_data( input, strlen(input) );
}

// duplicate a sha1
unsigned char* sha256_dup( unsigned char const* sha256 ) {
   unsigned char* ret = (unsigned char*)calloc( SHA256_DIGEST_LENGTH, 1 );
   memcpy( ret, sha256, SHA_DIGEST_LENGTH );
   return ret;
}

// compare two SHA256 hashes
int sha256_cmp( unsigned char const* sha256_1, unsigned char const* sha256_2 ) {
   if( sha256_1 == NULL )
      return -1;
   if( sha256_2 == NULL )
      return 1;
   
   return strncasecmp( (char*)sha256_1, (char*)sha256_2, SHA_DIGEST_LENGTH );
}


// make a sha-256 hash printable
char* sha256_printable( unsigned char const* sha256 ) {
   char* ret = (char*)calloc( sizeof(char) * (2 * SHA256_DIGEST_LENGTH + 1), 1 );
   char buf[3];
   for( int i = 0; i < SHA256_DIGEST_LENGTH; i++ ) {
      sprintf(buf, "%02x", sha256[i] );
      ret[2*i] = buf[0];
      ret[2*i + 1] = buf[1];
   }
   
   return ret;
}

// make a printable sha256 from data
char* sha256_hash_printable( char const* input, size_t len) {
   unsigned char* sha256 = sha256_hash_data( input, len );
   char* sha256_str = sha256_printable( sha256 );
   free( sha256 );
   return sha256_str;
}

// make a printable sha-1 hash into data
unsigned char* sha256_data( char const* sha256_printed ) {
   unsigned char* ret = (unsigned char*)calloc( SHA256_DIGEST_LENGTH, 1 );
   
   for( size_t i = 0; i < strlen( sha256_printed ); i+=2 ) {
      unsigned char tmp1 = (unsigned)from_hex( sha256_printed[i] );
      unsigned char tmp2 = (unsigned)from_hex( sha256_printed[i+1] );
      ret[i >> 1] = (tmp1 << 4) | tmp2;
   }
   
   return ret;
}


// hash a file
unsigned char* sha256_file( char const* path ) {
   FILE* f = fopen( path, "r" );
   if( !f ) {
      return NULL;
   }
   
   SHA256_CTX context;
   SHA256_Init( &context );
   unsigned char* new_checksum = (unsigned char*)calloc( SHA256_DIGEST_LENGTH, 1 );
   unsigned char buf[32768];
   
   ssize_t num_read;
   while( !feof( f ) ) {
      num_read = fread( buf, 1, 32768, f );
      if( ferror( f ) ) {
         errorf("sha256_file: I/O error reading %s\n", path );
         SHA256_Final( new_checksum, &context );
         free( new_checksum );
         fclose( f );
         return NULL;
      }
      
      SHA256_Update( &context, buf, num_read );
   }
   fclose(f);
   
   SHA256_Final( new_checksum, &context );
   
   return new_checksum;
}

// hash a file, given its descriptor 
unsigned char* sha256_fd( int fd ) {
   SHA256_CTX context;
   SHA256_Init( &context );
   unsigned char* new_checksum = (unsigned char*)calloc( SHA256_DIGEST_LENGTH, 1 );
   unsigned char buf[32768];
   
   ssize_t num_read = 1;
   while( num_read > 0 ) {
      num_read = read( fd, buf, 32768 );
      if( num_read < 0 ) {
         errorf("sha256_file: I/O error reading FD %d, errno=%d\n", fd, -errno );
         SHA256_Final( new_checksum, &context );
         free( new_checksum );
         return NULL;
      }
      
      SHA256_Update( &context, buf, num_read );
   }
   
   SHA256_Final( new_checksum, &context );
   
   return new_checksum;
}


// load a file into RAM
// return a pointer to the bytes.
// set the size.
char* load_file( char const* path, size_t* size ) {
   struct stat statbuf;
   int rc = stat( path, &statbuf );
   if( rc != 0 )
      return NULL;
   
   char* ret = (char*)calloc( statbuf.st_size, 1 );
   if( ret == NULL )
      return NULL;
   
   FILE* f = fopen( path, "r" );
   if( !f ) {
      free( ret );
      return NULL;
   }
   
   *size = fread( ret, 1, statbuf.st_size, f );
   fclose( f );
   return ret;
}

// read, but mask EINTR 
ssize_t md_read_uninterrupted( int fd, char* buf, size_t len ) {
   
   ssize_t num_read = 0;
   while( (unsigned)num_read < len ) {
      ssize_t nr = read( fd, buf + num_read, len - num_read );
      if( nr < 0 ) {
         
         int errsv = -errno;
         if( errsv == -EINTR ) {
            continue;
         }
         
         return errsv;
      }
      if( nr == 0 ) {
         break;
      }
      
      num_read += nr;
   }
   
   return num_read;
}


// recv, but mask EINTR 
ssize_t md_recv_uninterrupted( int fd, char* buf, size_t len, int flags ) {
   
   ssize_t num_read = 0;
   while( (unsigned)num_read < len ) {
      ssize_t nr = recv( fd, buf + num_read, len - num_read, flags );
      if( nr < 0 ) {
         
         int errsv = -errno;
         if( errsv == -EINTR ) {
            continue;
         }
         
         return errsv;
      }
      if( nr == 0 ) {
         break;
      }
      
      num_read += nr;
   }
   
   return num_read;
}

// write, but mask EINTR
ssize_t md_write_uninterrupted( int fd, char const* buf, size_t len ) {
   
   ssize_t num_written = 0;
   while( (unsigned)num_written < len ) {
      ssize_t nw = write( fd, buf + num_written, len - num_written );
      if( nw < 0 ) {
         
         int errsv = -errno;
         if( errsv == -EINTR ) {
            continue;
         }
         
         return errsv;
      }
      if( nw == 0 ) {
         break;
      }
      
      num_written += nw;
   }
   
   return num_written;
}


// send, but mask EINTR
ssize_t md_send_uninterrupted( int fd, char const* buf, size_t len, int flags ) {
   
   ssize_t num_written = 0;
   while( (unsigned)num_written < len ) {
      ssize_t nw = send( fd, buf + num_written, len - num_written, flags );
      if( nw < 0 ) {
         
         int errsv = -errno;
         if( errsv == -EINTR ) {
            continue;
         }
         
         return errsv;
      }
      if( nw == 0 ) {
         break;
      }
      
      num_written += nw;
   }
   
   return num_written;
}

// remove all files and directories within a directory, recursively.
// return errno if we fail partway through.
int md_clear_dir( char const* dirname ) {
   
   if(dirname == NULL) {
      return -EINVAL;
   }
   
   DIR *dirp = opendir(dirname);
   if (dirp == NULL) {
      int errsv = -errno;
      errorf("Failed to open %s, errno = %d\n", dirname, errsv);
      return errsv;
   }
   
   int rc = 0;
   ssize_t len = offsetof(struct dirent, d_name) + pathconf(dirname, _PC_NAME_MAX) + 1;
   
   struct dirent *dentry_dp = NULL;
   struct dirent *dentry = CALLOC_LIST( struct dirent, len );
   
   while(true) {
      
      rc = readdir_r(dirp, dentry, &dentry_dp);
      if( rc != 0 ) {
         errorf("readdir_r(%s) rc = %d\n", dirname, rc );
         break;
      }
      
      if( dentry_dp == NULL ) {
         // no more entries
         break;
      }
      
      // walk this directory.
      
      if(strncmp(dentry->d_name, ".", strlen(".")) == 0 || strncmp(dentry->d_name, "..", strlen("..")) == 0 ) {
         // ignore . and ..
         continue;
      }

      // path to this dir entry 
      char* path = md_fullpath( dirname, dentry->d_name, NULL );
      rc = 0;
      
      // if this is a dir, then recurse
      if(dentry->d_type == DT_DIR) {
         md_clear_dir( path );
         
         // blow away this dir 
         rc = rmdir(path);
         if( rc != 0 ) {
            rc = -errno;
            errorf("rmdir(%s) errno = %d\n", path, rc );
         }
      }
      else {
         // just unlink files 
         rc = unlink(path);
         if( rc != 0 ) {
            rc = -errno;
            errorf("unlink(%s) errno = %d\n", path, rc );
         }
      }
   
      free(path);
      path = NULL;
      
      // if we encountered an error, then abort 
      if( rc != 0 ) {
         break;
      }
   }
   
   // clean up
   closedir( dirp );
   free( dentry );
   
   return rc;
}

// create an AF_UNIX local socket 
// if bind_on is true, then this binds and listens on the socket
// otherwise, it connects
int md_unix_socket( char const* path, bool server ) {
   
   if( path == NULL ) {
      return -EINVAL;
   }
   
   struct sockaddr_un addr;
   int fd = 0;
   int rc = 0;
   
   memset(&addr, 0, sizeof(struct sockaddr_un));
   
   // sanity check 
   if( strlen(path) >= sizeof(addr.sun_path) - 1 ) {
      errorf("%s is too long\n", path );
      return -EINVAL;
   }
   
   // create the socket 
   fd = socket( AF_UNIX, SOCK_STREAM, 0 );
   if( fd < 0 ) {
      fd = -errno;
      errorf("socket(%s) rc = %d\n", path, fd );
      return fd;
   }
   
   // set up the sockaddr
   addr.sun_family = AF_UNIX;
   strncpy(addr.sun_path, path, strlen(path) );

   // server?
   if( server ) {
      // bind on it 
      rc = bind( fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un) );
      if( rc < 0 ) {
         rc = -errno;
         errorf("bind(%s) rc = %d\n", path, rc );
         close( fd );
         return rc;
      }
      
      // listen on it
      rc = listen( fd, 100 );
      if( rc < 0 ) {
         errorf("listen(%s) rc = %d\n", path, rc );
         close( fd );
         return rc;
      }
   }
   else {
      // client 
      rc = connect( fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un) );
      if( rc < 0 ) {
         rc = -errno;
         errorf("connect(%s) rc = %d\n", path, rc );
         close( fd );
         return rc;
      }
   }
   
   return fd;
}


// dump data to a temporary file.
// on success, allocate and return the path to the temporary file and return 0
// on failure, return negative and remove the temporary file that was created
int md_write_to_tmpfile( char const* tmpfile_fmt, char const* buf, size_t buflen, char** tmpfile_path ) {
   
   char* so_path = strdup( tmpfile_fmt );
   ssize_t rc = 0;
   
   int fd = mkstemp( so_path );
   if( fd < 0 ) {
      rc = -errno;
      errorf("mkstemp(%s) rc = %zd\n", so_path, rc );
      free( so_path );
      return rc;
   }
   
   // write it out
   rc = md_write_uninterrupted( fd, buf, buflen );
   
   close( fd );
   
   if( rc < 0 ) {
      // failure 
      unlink( so_path );
      free( so_path );
   }
   else {
      *tmpfile_path = so_path;
   }
   
   return rc;
}

//////// courtesy of http://www.geekhideout.com/urlcode.shtml  //////////


/* Returns a url-encoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *md_url_encode(char const *str, size_t len) {
  char *pstr = (char*)str;
  char *buf = (char*)calloc(len * 3 + 1, 1);
  char *pbuf = buf;
  size_t cnt = 0;
  while (cnt < len) {
    if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~') {
      *pbuf++ = *pstr;
    }
    else if (*pstr == ' ') {
      *pbuf++ = '+';
    }
    else {
      *pbuf++ = '%';
      *pbuf++ = to_hex(*pstr >> 4);
      *pbuf++ = to_hex(*pstr & 15);
    }
    pstr++;
    cnt++;
  }
  *pbuf = '\0';
  return buf;
}

/* Returns a url-decoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *md_url_decode(char const *str, size_t* len) {
  char *pstr = (char*)str, *buf = (char*)calloc(strlen(str) + 1, 1), *pbuf = buf;
  size_t l = 0;
  while (*pstr) {
    if (*pstr == '%') {
      if (pstr[1] && pstr[2]) {
        *pbuf++ = from_hex(pstr[1]) << 4 | from_hex(pstr[2]);
        pstr += 2;
        l++;
      }
    } 
    else if (*pstr == '+') { 
      *pbuf++ = ' ';
      l++;
    } 
    else {
      *pbuf++ = *pstr;
      l++;
    }
    pstr++;
  }
  *pbuf = '\0';
  l++;
  if( len != NULL ) {
     *len = l;
  }
  
  return buf;
}

//////////////////////////////////////////////////////////////////////////

// Base64 decode and encode, from https://gist.github.com/barrysteyn/4409525#file-base64decode-c

int calcDecodeLength(const char* b64input, size_t len) { //Calculates the length of a decoded base64 string
  int padding = 0;

  if (b64input[len-1] == '=' && b64input[len-2] == '=') //last two chars are =
    padding = 2;
  else if (b64input[len-1] == '=') //last char is =
    padding = 1;

  return (int)len*0.75 - padding;
}

int Base64Decode(const char* b64message, size_t b64message_len, char** buffer, size_t* buffer_len) { //Decodes a base64 encoded string
  BIO *bio, *b64;
  int decodeLen = calcDecodeLength(b64message, b64message_len);
  long len = 0;
  *buffer = (char*)malloc(decodeLen+1);
  FILE* stream = fmemopen((void*)b64message, b64message_len, "r");

  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new_fp(stream, BIO_NOCLOSE);
  bio = BIO_push(b64, bio);
  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); //Do not use newlines to flush buffer
  len = BIO_read(bio, *buffer, b64message_len);
    //Can test here if len == decodeLen - if not, then return an error
  (*buffer)[len] = '\0';

  BIO_free_all(bio);
  fclose(stream);

  *buffer_len = (size_t)len;

  return (0); //success
}


int Base64Encode(char const* message, size_t msglen, char** buffer) { //Encodes a string to base64
  BIO *bio, *b64;
  FILE* stream;
  int encodedSize = 4*ceil((double)msglen/3);
  *buffer = (char *)malloc(encodedSize+1);

  stream = fmemopen(*buffer, encodedSize+1, "w");
  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new_fp(stream, BIO_NOCLOSE);
  bio = BIO_push(b64, bio);
  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); //Ignore newlines - write everything in one line
  BIO_write(bio, message, msglen);
  (void)BIO_flush(bio);
  BIO_free_all(bio);
  fclose(stream);

  return (0); //success
}

//////////////////////////////////////////////////////////////////////////

// pseudo random number generator
static uint32_t Q[4096], c=362436; /* choose random initial c<809430660 and */
                                         /* 4096 random 32-bit integers for Q[]   */
                                         
pthread_mutex_t CMWC4096_lock = PTHREAD_MUTEX_INITIALIZER;

uint32_t CMWC4096(void) {
   pthread_mutex_lock( &CMWC4096_lock );
   
   uint64_t t, a=18782LL;
   static uint32_t i=4095;
   uint32_t x,r=0xfffffffe;
   
   i=(i+1)&4095;
   t=a*Q[i]+c;
   c=(t>>32);
   x=t+c;
   
   if( x < c ) {
      x++;
      c++;
   }
   
   Q[i]=r-x;
   
   uint32_t ret = Q[i];
   
   pthread_mutex_unlock( &CMWC4096_lock );
   return ret;
}

uint32_t md_random32(void) {
   return CMWC4096();
}

uint64_t md_random64(void) {
   uint64_t upper = md_random32();
   uint64_t lower = md_random32();
   
   return (upper << 32) | lower;
}


int util_init(void) {
   // pseudo random number init
   int rfd = open("/dev/urandom", O_RDONLY );
   if( rfd < 0 ) {
      return -errno;
   }

   ssize_t nr = read( rfd, Q, 4096 * sizeof(uint32_t) );
   if( nr < 0 ) {
      return -errno;
   }
   if( nr != 4096 * sizeof(uint32_t) ) {
      close( rfd );
      return -ENODATA;
   }
   
   close( rfd );
   return 0;
}


double timespec_to_double( struct timespec* ts ) {
   double ret = (double)ts->tv_sec + ((double)ts->tv_nsec) / 1e9;
   return ret;
}

double now_ns(void) {
   struct timespec ts;
   clock_gettime( CLOCK_REALTIME, &ts );
   
   return timespec_to_double( &ts );
}

// alloc and then mlock 
int mlock_calloc( struct mlock_buf* buf, size_t len ) {
   memset( buf, 0, sizeof( struct mlock_buf ) );
   
   int rc = posix_memalign( &buf->ptr, sysconf(_SC_PAGESIZE), len );
   if( rc != 0 ) {
      return rc;
   }
   
   memset( buf->ptr, 0, len );
   
   buf->len = len;
   
   rc = mlock( buf->ptr, buf->len );
   if( rc != 0 ) {
      free( buf->ptr );
      
      buf->ptr = NULL;
      buf->len = 0;
      
      return rc;
   }
   
   return 0;
}

// free an mlock'ed buf (unlock it first)
int mlock_free( struct mlock_buf* buf ) {
   if( buf->ptr != NULL ) {
      memset( buf->ptr, 0, buf->len );
      munlock( buf->ptr, buf->len );
      free( buf->ptr );
   }
   buf->ptr = NULL;
   buf->len = 0;
   return 0;
}

// duplicate a string into an mlock'ed buffer, allocating if needed
int mlock_dup( struct mlock_buf* dest, char const* src, size_t src_len ) {
   if( dest->ptr == NULL ) {
      int rc = mlock_calloc( dest, src_len );
      if( rc != 0 ) {
         errorf("mlock_calloc rc = %d\n", rc );
         return rc;
      }
   }
   else if( dest->len < src_len) {
      errorf("%s", "not enough space\n");
      return -EINVAL;
   }
   
   memcpy( dest->ptr, src, src_len );
   return 0;
}

// duplicate an mlock'ed buffer's contents, allocating dest if need be.
int mlock_buf_dup( struct mlock_buf* dest, struct mlock_buf* src ) {
   if( dest->ptr == NULL ) {
      int rc = mlock_calloc( dest, src->len );
      if( rc != 0 ) {
         errorf("mlock_calloc rc = %d\n", rc );
         return rc;
      }
   }
   else if( dest->len < src->len ) {
      errorf("%s", "not enough space\n");
      return -EINVAL;
   }
   
   memcpy( dest->ptr, src->ptr, src->len );
   return 0;
}



// flatten a response buffer into a byte string
char* response_buffer_to_string( response_buffer_t* rb ) {
   size_t total_len = 0;
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      total_len += (*rb)[i].second;
   }

   char* msg_buf = CALLOC_LIST( char, total_len );
   size_t offset = 0;
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      memcpy( msg_buf + offset, (*rb)[i].first, (*rb)[i].second );
      offset += (*rb)[i].second;
   }

   return msg_buf;
}

// free a response buffer
void response_buffer_free( response_buffer_t* rb ) {
   if( rb == NULL )
      return;
      
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      if( rb->at(i).first != NULL ) {
         free( rb->at(i).first );
         rb->at(i).first = NULL;
      }
      rb->at(i).second = 0;
   }
   rb->clear();
}

// size of a response buffer
off_t response_buffer_size( response_buffer_t* rb ) {
   off_t ret = 0;
   for( unsigned int i = 0; i < rb->size(); i++ ) {
      ret += rb->at(i).second;
   }
   return ret;
}
   

// get task ID (no glibc wrapper around this...)
pid_t gettid(void) {
   return syscall( __NR_gettid );
}
