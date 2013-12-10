#! /bin/sh
cython -a learnkernel.pyx
gcc -c -fPIC learnkernel.c -I /usr/include/python2.7
gcc -shared -o learnkernel.so learnkernel.o
