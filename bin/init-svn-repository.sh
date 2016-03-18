#!/usr/bin/env bash

# script to ease the initialization of an svn repository

SVN_DIR=${1-"/home/storage/svn/example"}
REMOTE_SVN=${2-"http://example.googlecode.com/svn/"}

[ -d $SVN_DIR ] && echo "$SVN_DIR already present. Do 'rm -rf $SVN_DIR' and relaunch this script if you really want to start from scratch." && exit 1

set -x

svnadmin create $SVN_DIR

cd $SVN_DIR

echo -e '#!/bin/sh\n' > hooks/pre-revprop-change
chmod +x hooks/pre-revprop-change

# Fill in some repository
svnsync init file://$SVN_DIR $REMOTE_SVN
svnsync sync file://$SVN_DIR
