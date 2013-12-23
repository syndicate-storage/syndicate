Syndicate
=========

Syndicate is an open-source cloud storage IP core, which implements a virtual private cloud storage system that scales.  You pick any combination of local storage, cloud storage, and edge caches (CDNs, HTTP object caches, caching proxies), and use Syndicate to turn them into an always-on, scalable read/write storage system tailored to your application's needs.

Syndicate offers filesystem-like semantics and consistency.  You choose how fresh file and directory data will be when you read or write it, and Syndicate takes care of the rest.  Tighter freshness will mean higher latency but stronger consistency, while looser freshness lead to lower latency but weaker consistency.  All the while, the transfer bandwidth is the same as what you would get from underlying storage and caches.  Syndicate **does NOT** rely on HTTP cache control directives--it enforces consistency itself, meaning you can use unmodified, already-deployed caches to help your application scale.

Syndicate is easily extensible and programmable.  You can have Syndicate use your favorate storage systems as back-ends and define storage-level data management policies with only a few lines of Python.  Syndicate comes with support for local disk, Amazon S3, Dropbox, Box.net, Google Cloud Storage, and Amazon Glacier.

What can I use Syndicate for?
-----------------------------

Here are a few examples of how you can use Syndicate.  We're actively working on these applications as separate projects, while using Syndicate as the common storage layer.

* Creating a [DropBox](http://www.dropbox.com)-like storage system that gives you transparent end-to-end encryption on top of your existing Dropbox account.
* Augmenting [Hadoop](http://hadoop.apache.com) with CDNs, so computing clusters across the world can automatically access and locally cache scientific data without having to manually download and install a local copy, and without having to worry about receiving stale data.  See the [HSynth](https://github.com/iychoi/hsynth) project for details.
* Adding HIPPA compliance on top of Amazon S3.
* Creating in-browser webmail with transparent end-to-end encryption, transparent key management, and transparent backwards compatibility with email (using Google Native Client to run the Syndicate client).
* Implementing scalable, secure VDI, using any combination of in-house and external storage and caches.
* Implementing vendor-agnostic cloud storage gateways.

Why use Syndicate of an existing cloud?
--------------------------------------------------------

Syndicate is **decentralized**.  You can distribute Syndicate across multiple clouds, multiple local networks, and multiple devices and servers.  Syndicate is not tied to any specific provider, and can tolerate a configurable number of server and provider failures.

Syndicate is **scalable**.  You can use Syndicate to share a few files across just your personal devices, or you can use it to distribute terabytes of data to millions of people.  You can increase scalability by simply giving it more caches and storage to work with.

Syndicate is **fast**.  Syndicate gives you the performance of the underlying clouds and CDNs you leverage.  You only pay a latency penalty when you need stronger consistency.

Syndicate is **flexible**.  It doesn't matter what underlying storage and caches you give to Syndicate; Syndicate can use them as back-ends.  Moreover, you can specify arbitrary storage policies and support arbitrary back-ends, and supports live policy and back-end upgrades for continuous availability.

Syndicate is **secure**.  Syndicate components use on strong, proven cryptography to authenticate to you and to one another.  A running system is tamper-resistent and lets you quickly add and revoke access to your data on a per-user, per-host, and per-file basis.  If you trust Syndicate, you don't have to trust your back-end storage and caches.

Building
--------

Syndicate uses the [SCons](http://www.scons.org/) build system.  To build Syndicate on your local system, you'll first need to install the packages listed in the [INSTALL](https://github.com/jcnelson/syndicate/blob/master/INSTALL) file.  Then, you'll need to build and install libsyndicate and the Syndicate Python library before building Syndicate.

To build and install libsyndicate and its headers to /usr/local, issue the following commands:

```
$ scons libsyndicate
$ sudo scons libsyndicate-install
```

To build and install the Syndicate Python library, issue the following commands:

```
$ scons python
$ sudo scons python-install
```

Finally, to build and install the various Syndicate components, issue the following commands:

```
$ scons syndicate
$ sudo scons syndicate-install
```

By default, everything will be installed to /usr/local.  You can override this by passing DESTDIR=.  For example:

```
$ sudo scons DESTDIR=/usr syndicate-install
```

Alternatively, you can get nightly RPMs from our [build server](http://vcoblitz-cmi.cs.princeton.edu/syndicate-nightly/RPMS/).  They're compiled for Fedora 12, since that's what we run on [PlanetLab](http://www.planet-lab.org).

Trying it out
-------------

TODO: set up an MS playground

More information
----------------

Take a look at our [wiki](https://github.com/jcnelson/syndicate/wiki) for how-tos and tutorials.
