Syndicate
=========

Syndicate is an **Internet-scale software-defined storage system**.  Syndicate presents a programmable abstraction layer over commodity cloud storage providers, external public datasets, and CDNs.  Unlike traditional cloud storage, Syndicate volumes have programable storage semantics and access controls. This lets you apply Syndicate to meet arbitrarily-specific storage needs, without worrying about the underlying technologies.

Cloud applications that use Syndicate do not need to host user data.  Instead, the application's server and client programs read and write data to user's Syndicate volumes.  This frees the application provider from liability and hosting burdens, and lets the user own the data he/she generates.

Syndicate competes with systems like [Tent](http://tent.io), [Freenet](https://freenetproject.org), [Unhosted](https://unhosted.org), and [Camlistore](https://camlistore.org).  The key differences are as follows:
* Syndicate volumes scale in the number of readers, writers, and files.  Users attach commodity cloud storage and CDN accounts to increase storage and bandwidth capacity.
* Syndicate has a fire-and-forget setup.  Users do not need to provision and manage their own servers to get started.
* Syndicate seamlessly integrates existing, external data into volumes in a zero-copy manner.
* Syndicate leverages CDNs and Web caches whenever possible, while providing guaranteed data consistency *even if caches return stale data.*
* Syndicate works with more than just Web applications.  Syndicate volumes are locally mountable as removable media, so *any* application can use them.
* Syndicate volumes have user-programmable storage semantics.  Reads and writes can have application-specific effects, such as automatic encryption, compression, deduplication, access logging, write-conflict resolution, customized authentication, etc.
* Users have built-in access control on a per-file, per-file-attribute, per-host, and per-user basis.

What can I use Syndicate for?
-----------------------------

Here are a few examples of how we are currently using Syndicate:

* Creating a secure [DropBox](http://www.dropbox.com)-like "shared folder" system for [OpenCloud](http://www.opencloud.us) that augments VMs, external scientific datasets, and personal computers with a private CDN, allowing users to share large amounts of data with their VMs while minimizing redundant transfers.
* Augmenting [Hadoop](http://hadoop.apache.com) with CDNs, so computing clusters across the world can transparently cache scientific data in commodity CDNs without having to manually re-host data or receive stale copies.  See the [H-Syndicate](https://github.com/iychoi/H-Syndicate) project for details.
* Adding [HIPPA](https://en.wikipedia.org/wiki/HIPAA) compliance on top of Amazon S3.
* Creating a decentralized video streaming service.
* Creating webmail with transparent end-to-end encryption, automatic key management, and backwards compatibility with email.  Email data gets stored encrypted to user-chosen storage service(s), so webmail providers like [Gmail](https://mail.google.com) can't snoop.  See the [SyndicateMail](https://github.com/jcnelson/syndicatemail) project for details.
* Implementing scalable secure VDI across the Internet, using both in-house and external storage and caches.
* Implementing vendor-agnostic [cloud storage gateways](https://en.wikipedia.org/wiki/Cloud_storage_gateway) on top of commodity hardware.

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
$ scons UG RG AG
$ sudo scons UG-install RG-install AG-install
```

By default, everything will be installed to /usr/local.  You can override this by passing DESTDIR=.  For example:

```
$ sudo scons DESTDIR=/usr UG-install
```

Advanced users can build individual Syndicate RPMs and debs with [fpm](https://github.com/jordansissel/fpm), using the build scripts [here](https://github.com/jcnelson/syndicate/tree/master/build/chroot).

Alternatively, you can get periodically-generated development snapshot from our [build server](http://www.cs.princeton.edu/~jcnelson/syndicate-nightly/).  They're compiled for Fedora 16 and Ubuntu 12.04.

Setting it up
-------------

Take a look at our [wiki](https://github.com/jcnelson/syndicate/wiki#getting-started) for how-tos and tutorials to get your personal Syndicate instance set up.
