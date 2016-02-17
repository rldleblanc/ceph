#!/bin/sh
set -x

#GCCFLAGS="-std=c++11 -I. -g"
GCCFLAGS="-std=c++11 -g -I. -Iinclude/ -O0 -lboost_system -lboost_iostreams"

g++ $GCCFLAGS -c RunningStat.cc -o RunningStat.o
g++ $GCCFLAGS -c wrr_bench.cc -o wrr_bench.o
g++ $GCCFLAGS -o wrr_bench wrr_bench.o RunningStat.o common/assert.o common/Clock.o common/dout.o common/BackTrace.o common/version.o common/Thread.o common/io_priority.o common/PrebufferedStreambuf.o common/page.o common/signal.o common/code_environment.o common/errno.o common/safe_io.o log/Log.o /usr/lib/x86_64-linux-gnu/libpthread.so common/Graylog.o common/Formatter.o msg/msg_types.o common/LogEntry.o common/escape.o common/buffer.o common/HTMLFormatter.o common/entity_name.o common/simple_spin.o common/strtol.o common/libcommon_crc_la-*.o common/environment.o common/armor.o arch/probe.o arch/intel.o arch/arm.o common/ceph_strings.o
