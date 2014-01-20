Syndicate
=========

Syndicate is an **open source virtual cloud storage system**.  You pick any combination of unmodified local storage, existing cloud storage, and Web caches (CDNs, HTTP object caches, caching proxies), and use Syndicate to combine them into an always-on, scalable read/write storage medium (a "cloud") you can access as a locally-mounted filesystem.

Overview
--------

Today, when people think of cloud storage, they think of cloud storage providers like Google, Amazon, or Dropbox.  These providers will sell you storage space, but they force you to access it with their APIs, locking you into their platforms.  Worse, they can change these APIs on a whim (breaking your applications), change their prices, read your data, or go bankrupt, taking your data with them.

In a similar vein, [content delivery network](https://en.wikipedia.org/wiki/Content_delivery_network) providers such as [Akamai](http://www.akamai.com) and [CloudFlare](https://www.cloudflare.com/) will sell you edge caching capacity, allowing your applications to serve lots of data to lots users at once.  However, a well-known limitation of Web caches (including CDNs) is that they can serve your clients stale data.  Worse, Web cache operators ultimately decide what constitutes "stale" data, regardless of your [HTTP cache-control directives](https://en.wikipedia.org/wiki/Cache-Control#Controlling_Web_caches).

Syndicate solves both problems by implementing a layer of abstraction around both cloud storage and Web cache providers.  Syndicate organizes your data into one or more filesystem-like volumes, and lets you control how fresh file and directory data must be *independent of cache controls*.  By enforcing consistency itself, *Syndicate lets you use unmodified, already-deployed clouds and caches to help your applications scale*.

Syndicate is easily extensible and programmable.  You can make Syndicate use your favorate storage systems for hosting data, and define storage-level data management policies with only a few lines of Python code.  Syndicate comes with support for local disk, [Amazon S3](https://aws.amazon.com/s3/), [Dropbox](http://www.dropbox.com), [Box.net](http://www.box.net), [Google Cloud Storage](https://cloud.google.com/products/cloud-storage/), and [Amazon Glacier](https://aws.amazon.com/glacier/).

What can I use Syndicate for?
-----------------------------

Here are a few examples of how we are currently using Syndicate:

* Creating a [DropBox](http://www.dropbox.com)-like storage system for [PlanetLab](http://www.planet-lab.org) that augments a PlanetLab VM with a private CDN, allowing you to push out large amounts of data to the wide-area without exceeding bandwidth caps.
* Augmenting [Hadoop](http://hadoop.apache.com) with CDNs, so computing clusters across the world can automatically access and locally cache scientific data without having to manually download and install local copies, and without having to worry about receiving stale data.  See the [HSynth](https://github.com/iychoi/hsynth) project for details.
* Adding [HIPPA](https://en.wikipedia.org/wiki/HIPAA) compliance on top of Amazon S3.
* Creating a decentralized video streaming service.
* Creating webmail with transparent end-to-end encryption, automatic key management, and backwards compatibility with email.  Email data gets stored encrypted to cloud storage of your choice, so webmail providers like [Gmail](https://mail.google.com) can't snoop.  See the [SyndicateMail](https://github.com/jcnelson/syndicatemail) project for details.
* Implementing scalable secure VDI, using both in-house and external storage and caches.
* Implementing vendor-agnostic [cloud storage gateways](https://en.wikipedia.org/wiki/Cloud_storage_gateway) on top of commodity hardware.

Why use Syndicate over an existing cloud?
-----------------------------------------

Syndicate is **provider-agnostic**.  You can distribute Syndicate across multiple clouds, multiple local networks, and multiple devices and servers.  Syndicate is not tied to any specific provider, and can tolerate a configurable number of server and provider failures.

Syndicate is **scalable**.  You can use Syndicate to share a few files across just your personal devices, or you can use it to distribute terabytes of data to millions of people.  You can increase scalability by simply giving it more caches and storage to work with.

Syndicate is **fast**.  Syndicate gives you the performance of the underlying clouds and CDNs you leverage.  You only pay a latency penalty when you need stronger consistency.

Syndicate is **flexible**.  It doesn't matter what underlying storage and caches you give to Syndicate; Syndicate can use them as back-ends.  Moreover, you can specify arbitrary storage policies and support arbitrary back-ends, and supports live policy and back-end upgrades for continuous availability.

Syndicate is **secure**.  Syndicate components use strong cryptography to authenticate to you and to one another.  A running system is tamper-resistent and lets you quickly add and revoke access to your data on a per-user, per-host, and per-file basis.  If you trust Syndicate, you don't have to trust your back-end storage and caches.

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
