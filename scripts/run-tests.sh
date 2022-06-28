#!/bin/bash

HOME_DIR=/mnt/Work/CS739-P3
cd $HOME_DIR

CONF_FILE=$HOME_DIR/resources/exec.conf    # Need absolute path for conf file
LOGS_DIR=$HOME_DIR/logs
LOG_FILE=$LOGS_DIR/client_test.log
STORE_DIRS=$HOME_DIR/store*/*

mkdir -p $LOGS_DIR
rm -rf $STORE_DIRS

bazel build //server:server --cxxopt=-std=c++17 --cxxopt=-DCRASH_TEST

echo "Running tests..."
bazel run //client:client_test --cxxopt=-std=c++17 --cxxopt=-DCRASH_TEST -- $CONF_FILE > $LOG_FILE 2>&1
echo "Tests completed."
echo "View log file $LOG_FILE for more details."
echo "Use preprocessor CRASH_TEST_DEBUG for verbose logging."

rm -rf $STORE_DIRS

