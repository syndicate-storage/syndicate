/*
   Copyright 2013 The Trustees of Princeton University
   All Rights Reserved
*/

#include "AG.h"
#include "util.h"

int main( int argc, char** argv ) {
   block_all_signals();
   int rc = AG_main( argc, argv );
   return rc;
}

