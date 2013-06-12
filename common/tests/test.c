#include "libsyndicate.h"

int main(int argc, char** argv) {
   struct md_entry ent;
   memset( &ent, 0, sizeof(struct md_entry) );

   md_entry_create( &ent, "http://www.cs.princeton.edu/~jcnelson/index.html", "/home/jcnelson/index.html", NULL, 1332828266, 1, 12345, 0644, 14441, sha1_data( "b72ff11f6af13b6db07942c81ca99e942fc3ab99" ) );

   char* s = md_to_string( &ent, NULL );
   printf("md_entry_create: %s\n", s );

   ent.url_replicas = CALLOC_LIST( char*, 3 );
   ent.url_replicas[0] = strdup( "http://s3.amazon.com/home/jcnelson/index.html" );
   ent.url_replicas[1] = strdup( "http://vcoblitz-cmi.cs.princeton.edu/backups/home/jcnelson/index.html" );

   free( s );
   s = md_to_string( &ent, NULL );

   printf("with url replicas: %s\n", s );

   FILE* f = fopen("/tmp/.libsyndicate.test", "w+");
   if( f ) {
      int rc = md_write_entry( f, &ent );
      fclose(f);

      if( rc != 0 )
         fprintf(stderr, "Could not write test file\n");
      else {
         struct md_entry ent2;
         memset(&ent2, 0, sizeof(ent2) );
         rc = md_read_entry2( "/tmp/.libsyndicate.test", &ent2 );
         if( rc != 0 )
            fprintf(stderr, "Could not read test file\n");
         else {
            s = md_to_string( &ent2, NULL );
            printf("wrote and recovered: %s", s );
         }
      }
   }
   else {
      fprintf(stderr, "Could not open test file\n");
   }

   
   return 0;
}
