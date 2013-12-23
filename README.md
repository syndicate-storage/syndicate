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

Why use Syndicate of an existing cloud storage provider?
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

Alternatively, you can get nightly RPMs from our (build server)[http://vcoblitz-cmi.cs.princeton.edu/syndicate-nightly/RPMS/].  They're compiled for Fedora 12, since that's what we run on [PlanetLab](http://www.planet-lab.org).

Deploying Syndicate
-------------------

Before you do anything, you (or someone you trust) will first need to get a Syndicate Metadata Service ([MS](https://github.com/jcnelson/syndicate/tree/master/ms)) up and running.  If you're going to be using an existing MS, you can skip this section.

The MS acts as an always-on coordination service for Syndicate's wide-area peers.  You can install it into [Google AppEngine](https://developers.google.com/appengine/) as a Python app; you can install it into an [AppScale](http://www.appscale.com) instance on your servers; or you can run it locally with the [Google AppEngine SDK](https://developers.google.com/appengine/docs/python/tools/devserver).

Before you deploy your MS, you will need to set up your administrator account.  To do so, you'll need an RSA 4096-bit key.  You can generate an RSA 4096-bit key pair with these commands:

```
$ openssl genrsa -out /path/to/your/admin/key.pem 4096
$ openssl rsa -in /path/to/your/admin/key.pem pubout > /path/to/your/admin/public/key.pub
```

Then, you can set up the administrator account with this command:

```
$ scons MS-setup-admin email=your.email@example.com key=/path/to/your/admin/public/key.pub
```

Before you deploy the MS, you'll need to generate an app.yaml file.  The only thing Syndicate needs from you is the application name to use.  To generate the file, run this command:

```
$ scons MS-setup-app name=YOUR-APP-NAME
```

Now you can deploy the MS.  For example, to deploy to Google AppEngine, you can run the appcfg.py script from the source root directory:

```
$ appcfg.py update build/out/ms
```
