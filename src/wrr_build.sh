#!/bin/sh
set -x

#GCCFLAGS="-std=c++11 -I. -g"
GCCFLAGS="-std=c++11 -I. -O3"

g++ $GCCFLAGS -c RunningStat.cc -o RunningStat.o
g++ $GCCFLAGS -c wrr_bench.cc -o wrr_bench.o
g++ $GCCFLAGS -o wrr_bench wrr_bench.o RunningStat.o common/assert.o common/Clock.o common/dout.o common/BackTrace.o common/version.o common/Thread.o common/io_priority.o common/PrebufferedStreambuf.o common/page.o common/signal.o common/code_environment.o common/errno.o common/safe_io.o log/Log.o /usr/lib/x86_64-linux-gnu/libpthread.so
