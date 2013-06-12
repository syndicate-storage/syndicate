#!/bin/sh

# Build libmicrohttpd in preparation for building Syndicate

OUTDIR=$(pwd)/out

if [[ $1 == "clean" ]]; then
   rm -rf $OUTDIR
   exit 0
fi

test -d $OUTDIR/include && test -d $OUTDIR/lib && test -f $OUTDIR/lib/libmicrohttpd.a && exit 0

./bootstrap && \
./configure --prefix=$OUTDIR && \
make all && \
make install && \
cd $OUTDIR/lib && \
ar x libmicrohttpd.a
