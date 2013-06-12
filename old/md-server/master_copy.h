/*
 * Depricated; will not be used in the future
 */

#ifndef _MASTER_COPY_H_
#define _MASTER_COPY_H_

#include "libsyndicate.h"
#include "validator.h"

#define MASTERCOPY_THREAD_WORKSIZE 10000

using namespace std;

typedef void (*consume_func)(struct md_entry* ent, void* cls);

// Threadpool for walking the master copy
class MasterCopy : public Threadpool<struct md_entry> {
public:
   MasterCopy( struct md_syndicate_conf* conf, consume_func cf, void* cls );
   ~MasterCopy();
   
   // set our consumer
   void set_consumer_cls( void* cls ) { this->cls = cls; }
   
   // begin walking the master copy
   int begin();
   
   // wait until we're done walking the master copy
   int wait( int check_interval );
   
   // process a directory entry, and enqueue more master copy paths
   int process_work( struct md_entry* ent, int thread_no );
   
   // override kill() to set done
   int kill(int sig) {
      this->done = true;
      return Threadpool<struct md_entry>::kill( sig );
   }
   
   // we will have work as long as we're walking the master copy
   bool has_more();
   
private:
   
   struct md_syndicate_conf* conf;
   consume_func consumer;
   void* cls;
   bool done;                    // set to true once we're out of work
};

#endif
