#!/bin/bash
export VERSION="2.2.5"
export BUILD_NAME="0"
export CONDA_BLD_PATH=~/conda-bld
USER="e3sm"
PLATFORM="noarch"
PKG="processflow"

if [ -d $CONDA_BLD_PATH ]; then
    rm -rf $CONDA_BLD_PATH
fi
echo "Creating build dir at" $CONDA_BLD_PATH
mkdir $CONDA_BLD_PATH

conda config --set anaconda_upload no
if [ ! -z "$1" ]; then
    export TAG="$1"
else
    export TAG="master"
fi
echo "Building" $VERSION"-"$BUILD_NAME "for label:" $TAG

conda build -c conda-forge -c e3sm -c cdat/label/v82 . 

if [ $? -eq 1 ]; then
    echo "conda build failed"
    exit
fi

if [ ! -z "$1" ]; then
    anaconda upload -u $USER -l "$1" $CONDA_BLD_PATH/$PLATFORM/$PKG-$VERSION-$BUILD_NAME.tar.bz2 
else
    anaconda upload -u $USER $CONDA_BLD_PATH/$PLATFORM/$PKG-$VERSION-$BUILD_NAME.tar.bz2
fi
