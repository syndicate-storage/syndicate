Syndicate
=========

Syndicate is an open-source cloud storage IP core, which implements a virtual private cloud storage system that scales.  You pick any combination of local storage, cloud storage, and edge caches (CDNs, HTTP object caches, caching proxies), and use Syndicate to turn them into an always-on, scalable read/write storage system tailored to your application's needs.

Syndicate offers filesystem-like semantics and consistency.  You choose how fresh file and directory data will be when you read or write it, and Syndicate takes care of the rest.  Tighter freshness requiremenets will incur higher latency, and looser freshness requiremenets will incur lower latency.  All the while, the bandwidth is exactly the same as what you would get from underlying storage and caches.  Syndicate **does NOT** rely on HTTP cache control directives--it enforces consistency itself, meaning you can use unmodified, already-deployed caches to help your application scale.

Syndicate is easily extensible and programmable.  You can add support for multiple back-end storage systems and define storage-level data management policies on top of them with only a few lines of Python.  Syndicate comes with support for local disk, Amazon S3, Dropbox, Box.net, Google Cloud Storage, and Amazon Glacier.

What can I use Syndicate for?
-----------------------------

* Creating a DropBox-like storage system with transparent end-to-end encryption, using your existing Dropbox account.
* Augmenting Hadoop's HDFS with CDNs, allowing clusters across the world to access and locally cache large but changing scientific datasets, both without having to manually download and install a local copy, and without having to worry about receiving stale data.
* Creating in-browser webmail with transparent end-to-end encryption, transparent key management, and transparent backwards compatibility with email.
* Implementing scalable, secure VDI, using any combination of in-house and external storage and caches.
* Implementing vendor-agnostic cloud storage gateways.

The authors of Syndicate are working on all of the above, in addition to developing Syndicate.

Why use Syndicate?
------------------

Syndicate is **decentralized**.  You can distribute Syndicate across multiple clouds, multiple local networks, and multiple devices.  Syndicate is not tied to any specific provider.

Syndicate is **scalable**.  You can use Syndicate to share a few files across just your personal devices, or you can use it to distribute terabytes of data to millions of people.  You can increase scalability by simply giving it more caches and storage to work with.

Syndicate is **flexible**.  It doesn't matter what underlying storage and caches you give to Syndicate; Syndicate will combine them all into a single, virtual cloud that looks and acts the same regardless of your back-ends.  Moreover, Syndicate lets you specify arbitrary storage policies and support arbitrary back-ends, and supports live upgrades for continuous availability.

Syndicate is **secure**.  Syndicate components rely on strong, proven cryptography to authenticate to you and to one another.  A running system is tamper-resistent and lets you quickly add and revoke access to your data on a per-user, per-host, and per-file basis.  If you trust Syndicate, you don't have to trust your back-end storage and caches.

Building
--------

To build Syndicate on your local system, you'll first need to install the packages listed in the INSTALL file.  Then, issue the following commands to build and install Syndicate:

```
$ scons libsyndicate libsyndicate-python
$ sudo scons libsyndicate-install libsyndicate-python-install
$ scons ms ms-clients RG RG-python UG AG AG-drivers
$ sudo scons RG-install UG-install AG-install
```

Architecture
------------

