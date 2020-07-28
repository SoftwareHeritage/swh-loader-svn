#!/usr/bin/env bash

# Reference steps to install pysvn from tarballs served at
# http://pysvn.barrys-emacs.org/

# system pre-requisites
sudo apt-get install libaprutil1-dev libapr1-dev libsvn1

# on to the installation
mkdir /tmp/pysvn-build && cd /tmp/pysvn-build

wget http://pysvn.barrys-emacs.org/source_kits/pysvn-1.8.0.tar.gz

tar xvf pysvn-1.8.0.tar.gz

cd pysvn-1.8.0/Source/

python3 setup.py configure --svn-lib-dir=/usr/lib/x86_64-linux-gnu --apr-inc-dir=/usr/include/apr-1.0 --apu-inc-dir=/usr/include/apr-1.0 --apr-lib-dir=/usr/lib/x86_64-linux-gnu

make

mkdir -p ~/.local/lib/python3.5/

cp -rv pysvn/  ~/.local/lib/python3.5/
