#!/bin/sh

# must define:
# * BUILD_DIR
# * SYNDICATE_DIR
# * PKG_DIR
# * SCRIPTS_DIR
# * LOGS_DIR
source ./nightly.conf
source /usr/local/rvm/scripts/rvm

SCRIPTS=$(ls $SCRIPTS_DIR)

mkdir -p $BUILD_DIR
mkdir -p $PKG_DIR
rm -rf $BUILD_DIR/*
rm -rf $PKG_DIR/*

LOGFILE=$LOGS_DIR/nightly.log.$(date +%Y\%m\%d\%H\%M)
for script in $SCRIPTS; do
   script=$SCRIPTS_DIR/$script
   sh $script $BUILD_DIR $PKG_DIR $SYNDICATE_DIR 2>&1 >> $LOGFILE
done


