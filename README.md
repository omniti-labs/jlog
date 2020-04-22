# JLog

[Change](ChangeLog.md)

JLog is short for "journaled log" and this package is really an API and implementation that is libjlog.
What is libjlog? libjlog is a pure C, **very simple** durable message queue with multiple subscribers and publishers
(both thread- and multi-process safe). The basic concept is that publishers can open a log and write messages to it
while subscribers open the log and consume messages from it. "That sounds easy." libjlog abstracts away the need to
perform log rotation or maintenance by publishing into fixed size log buffers and eliminating old log buffers when
there are no more consumers pending.

## Implementation Goal

The goal of libjlog is to provide a dead-simple API, and a very small, uncomplicated implementation. 
The goal of the JLog is to have others use this project for adding durability to some of their asynchronous 
notification needs.

## Sample Use Case

We run an application on server A and would like to write logs to server B. If server B is down, we don't want to lose
logs and we do want A to continue operating. So A writes to a JLog and B consumes from that JLog. Sounds simple, but JLog
is a low-level API. The implementor must provide a service on A to which B can connect to consume the JLog messages over
the network. If you want to have you cake and eat it too, become a baker and use this fine ingredient!

Read through [Concepts](./CONCEPTS.md) for a bit more detail on core concepts and a brief overview of how to use the C API.

## Installing

FreeBSD port `databases/jlog`:

        pkg install jlog

MacOS via [Homebrew](http://brew.sh/):

        brew install jlog

If JLog is not packaged by your OS vendor, read on for build instructions.

The LZ4 library is required if you want support for compressed logs.

Java 6 or newer is required if you want to support using JLog from a Java application.

To build from source, clone this repo and then:

    autoconf
    ./configure
    make
    make install

## Team

* Wez Furlong
* Alec Peterson
* George Schlossnagle
* Theo Schlossnagle (current maintainer)
* Alexey Toptygin

## License

JLog is released under a new BSD license. See our [license](./LICENSE) for details.
