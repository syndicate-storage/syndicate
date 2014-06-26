Syndicate
=========

Syndicate is an **Internet-scale software-defined storage system**.  You pick a combination of unmodified, existing cloud storage, dataset, and edge cache providers, and use Syndicate to turn them into an always-on, scalable read/write storage medium (a "cloud") that has exactly the domain-specific functionality your application needs.

Today, providers force you to use their APIs.  This not only locks your application into their platforms, but also puts you at their mercy for price and API changes.  This is unacceptable for your wallet and your application's stability, especially if your application relies on specific storage-level features they offer (like transparent encryption, access logging, replication, data retention policy enforcement, etc.).

Syndicate solves this problem by implementing a scalable, programmable, and trustworthy storage fabric that separates your application from these providers, but with neglegible performance overhead.  You write one or more provider-agnostic storage drivers to implement your desired storage functionality, and Syndicate transparently and securely applies them over your storage providers.  With Syndicate, it doesn't matter which providers you use--you're no longer dependent on any particular one, so you can mix and match based on your cost/performance needs without rewriting your application.

What can I use Syndicate for?
-----------------------------

Here are a few examples of how we are currently using Syndicate:

* Creating a [DropBox](http://www.dropbox.com)-like storage system for [PlanetLab](http://www.planet-lab.org) that augments a PlanetLab VM with a private CDN, allowing you to push out large amounts of data to the wide-area without exceeding bandwidth caps.
* Augmenting [Hadoop](http://hadoop.apache.com) with CDNs, so computing clusters across the world can automatically access and locally cache scientific data without having to manually download and install local copies, and without having to worry about receiving stale data.  See the [H-Syndicate](https://github.com/iychoi/H-Syndicate) project for details.
* Adding [HIPPA](https://en.wikipedia.org/wiki/HIPAA) compliance on top of Amazon S3.
* Creating a decentralized video streaming service.
* Creating webmail with transparent end-to-end encryption, automatic key management, and backwards compatibility with email.  Email data gets stored encrypted to cloud storage of your choice, so webmail providers like [Gmail](https://mail.google.com) can't snoop.  See the [SyndicateMail](https://github.com/jcnelson/syndicatemail) project for details.
* Implementing scalable secure VDI, using both in-house and external storage and caches.
* Implementing vendor-agnostic [cloud storage gateways](https://en.wikipedia.org/wiki/Cloud_storage_gateway) on top of commodity hardware.

Why use Syndicate over an existing cloud?
-----------------------------------------

Syndicate is **provider-agnostic**.  You can distribute Syndicate across multiple clouds, multiple local networks, and multiple devices and servers.  Syndicate is not tied to any specific provider, and can tolerate a configurable number of server and provider failures.

Syndicate is **consistent**.  It has several data consistency models built-in (with room for extension), and keeps your data consistent even in the face of weak cache or cloud consistency.  You don't have to worry about reading stale or corrupt data.

Syndicate is **scalable**.  You can use Syndicate to share a few files across just your personal devices, or you can use it to distribute terabytes of data to millions of people.  You can increase scalability by simply giving it more caches and storage to work with--it scales along with your application.

Syndicate is **fast**.  Syndicate gives you the performance of the underlying clouds and CDNs you leverage.  You only pay a latency penalty when you need stronger consistency.

Syndicate is **flexible**.  It doesn't matter what underlying providers you give to Syndicate--the architecture is flexible enough to support arbitrary back-ends and arbitrary storage functionality.  Storage drivers are designed to compose through pipelining, so you can mix and match storage features to meet your exact needs.  In addition, it offers a multiple front-end APIs, which remain stable in the face of back-end changes.

Syndicate is **secure**.  Syndicate's storage fabric uses strong cryptography to make the system tamper-resistent while letting you quickly add and revoke access to your data on a per-user, per-host, and per-file basis.  If you trust Syndicate, you don't have to trust your back-end providers.

Where can I learn more?
-----------------------

Please see a look at our [whitepaper](https://www.cs.princeton.edu/~jcnelson/acm-bigsystem2014.pdf), published in the proceedings of the 1st International Workshop on Software-defined Ecosystems (colocated with HPDC 2014).

Building
--------

Syndicate uses the [SCons](http://www.scons.org/) build system.  To build Syndicate on your local machine, you'll first need to install the packages listed in the [INSTALL](https://github.com/jcnelson/syndicate/blob/master/INSTALL) file.  Then, you'll need to build and install libsyndicate and the Syndicate Python library before building Syndicate.

To build and install libsyndicate and its headers to /usr/local, issue the following commands:

```
$ scons libsyndicate
$ sudo scons libsyndicate-install
```

To build and install the Syndicate Python library, issue the following commands:

```
$ scons libsyndicateUG
$ sudo scons libsyndicateUG-install
$ scons syndicate-python
$ sudo scons syndicate-python-install
```

Finally, to build and install the various Syndicate components, issue the following commands:

```
$ scons AG-common
$ sudo scons AG-common-install
$ scons UG RG AG
$ sudo scons UG-install RG-install AG-install
```

By default, everything will be installed to /usr/local.  You can override this by passing DESTDIR=.  For example:

```
$ sudo scons DESTDIR=/usr UG-install
```

Alternatively, you can get nightly RPMs from our [build server](http://vcoblitz-cmi.cs.princeton.edu/syndicate-nightly/RPMS/).  They're compiled for Fedora 12, since that's what we use to run it on [PlanetLab](http://www.planet-lab.org).

Setting it up
-------------

Take a look at our [wiki](https://github.com/jcnelson/syndicate/wiki#getting-started) for how-tos and tutorials to get your personal Syndicate instance set up.
