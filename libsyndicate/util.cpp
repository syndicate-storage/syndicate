/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

/*
 * Utility functions (debugging, etc)
 */
 
#include "util.h"

int _DEBUG = 1;

int _DEBUG_MESSAGES = 1;
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


// make a directory sanely
int mkdir_sane( char* dirpath ) {
   DIR* dir = opendir( dirpath );
   if( dir == NULL && errno == ENOENT ) {
      // the directory does not exist, so try making it
      int old = umask( ~(0700) );
      int rc = mkdir( dirpath, 0700 );
      umask( old );
      if( rc != 0 && errno != EEXIST ) {
         return -errno;     // couldn't make the directory, and it wasn't because it already existed.
      }
   }
   else if( dir == NULL ) {
      // some other error
      return -errno;
   }
   else {
      // close the directory; we can write to it
      closedir( dir );
      return 0;
   }
   return 0;
}


// remove a directory sanely.
// rf = true means that the function will recursively remove all files
int rmdir_sane( char* dirpath, bool rf ) {
   //logerr("ERR: rmdir_sane not implemented\n");
   return 0;
}

// check and see whether or not a directory exists
int dir_exists( char* dirpath ) {
   DIR* dir = opendir( dirpath );
   if( dir == NULL )
      // doesn't exist or we can't read it
      return -errno;
   
   closedir( dir );
   return 0;      // it existed when we checked
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

//////// courtesy of http://www.geekhideout.com/urlcode.shtml  //////////


/* Returns a url-encoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
char *url_encode(char const *str, size_t len) {
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
char *url_decode(char const *str, size_t* len) {
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

// does a string match a pattern?
int reg_match(const char *string, char const *pattern) {
   int status;
   regex_t re;

   if( regcomp(&re, pattern, REG_EXTENDED|REG_NOSUB) != 0) {
      return 0;
   }
   status = regexec(&re, string, (size_t)0, NULL, 0);
   regfree(&re);
   if (status != 0) {
      return 0;
   }
   return 1;
}

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

void block_all_signals() {
    sigset_t sigs;
    sigfillset(&sigs);
    pthread_sigmask(SIG_SETMASK, &sigs, NULL);
}

int install_signal_handler(int signo, struct sigaction *act, sighandler_t handler) {
    int rc = 0;
    sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, signo);
    act->sa_handler = handler;
    rc = sigaction(signo, act, NULL);
    if (rc < 0)
	return rc;
    rc = pthread_sigmask(SIG_UNBLOCK, &sigs, NULL);
    return rc;
}

int uninstall_signal_handler(int signo) {
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

