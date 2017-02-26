#!/bin/bash

set -x
rm -rf publish lib/*
SOURCE_DIR=`pwd`

BUILD_TYPE="debug"
if [ $# -le 0 ] ; then 
    echo "use default debug mode"
elif [ $# -eq 1 ] ; then
	if [[ $1 != "debug" ]] && [[ $1 != "release" ]] ; then
	     echo "usage ./build.sh debug OR ./build.sh release"
	     exit;
	fi
	BUILD_TYPE=$1
else
    echo "parameter invalid,$#,$*";
    echo "usage ./build.sh debug OR ./build.sh release"
	exit
fi

BUILD_DIR=${BUILD_DIR:-./publish}
BUILD_TYPE=${BUILD_TYPE:-debug}
INSTALL_DIR=${INSTALL_DIR:-../${BUILD_TYPE}-install}
BUILD_NO_EXAMPLES=${BUILD_NO_EXAMPLES:-0}

mkdir -p $BUILD_DIR/$BUILD_TYPE \
  && cd $BUILD_DIR/$BUILD_TYPE \
  && cmake \
           -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
           -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
           -DCMAKE_BUILD_NO_EXAMPLES=$BUILD_NO_EXAMPLES \
           --no-warn-unused-cli \
           $SOURCE_DIR \
  && make

