Syndicate
=========

Syndicate is an **Internet-scale software-defined storage system**.  Syndicate implements a scalable, programmable wide-area storage layer over commodity cloud storage providers, external public datasets, and CDNs.  Unlike traditional cloud storage, Syndicate volumes have *programable storage semantics*. This lets you deploy Syndicate to meet arbitrarily-specific storage needs with a minimal amount of effort.

Networked applications that use Syndicate do not need to host user data.  Instead, users attach their volumes to the application, and the application reads and writes data to them instead.  This frees the application provider from liability and hosting burdens, and lets the user own the data he/she generates.

Here is an incomplete summary of features Syndicate offers:
* **Syndicate volumes scale up** in the number of readers, writers, and files.  Users attach commodity cloud storage and CDN capacity to increase storage and bandwidth.
* Syndicate has a **fire-and-forget setup**.  Users do not need to provision and manage their own servers to get started.
* Syndicate **seamlessly integrates existing data** by incorporating it into volumes in a zero-copy manner.
* Syndicate leverages CDNs and Web caches whenever possible, providing **caching with guaranteed consistency** even when caches return stale data.
* Syndicate works with more than just Web applications.  Syndicate volumes are **locally mountable as removable media**, so *any* application can use them.
* Syndicate volumes have **programmable semantics**.  I/O operations have application-defined side-effects, such as automatic encryption, compression, deduplication, access logging, write-conflict resolution, customized authentication, and so on.  Moroever, the programming model (inspired by the UNIX pipeline) ensures that storage semantics are *composable*--if A(x) and B(x) are functions on I/O operation x, then A(B(x)) is as well.
* Syndicate volumes have built-in access controls on a per-file, per-file-attribute, per-host, and per-user basis.  Users can extend them in a provider-agnostic way by changing how they are interpreted by add-on storage logic.

What can I use Syndicate for?
-----------------------------

Here are a few examples of how we are currently using Syndicate:

* Augmenting scientific storage systems (like [iRODS](https://irods.org)) and public datasets (like [GenBank](https://www.ncbi.nlm.nih.gov/genbank/)) with ingress Web caches in order to automatically stage large-scale datasets for compute resources. 
* Creating a secure [DropBox](http://www.dropbox.com)-like "shared folder" system for [OpenCloud](http://www.opencloud.us) that augments VMs, external scientific datasets, and personal computers with a private CDN, allowing users to share large amounts of data with their VMs while minimizing redundant transfers.
* Scalably packaging up and deploying applications across the Internet.
* Creating webmail with transparent end-to-end encryption, automatic key management, and backwards compatibility with email.  Email data gets stored encrypted to user-chosen storage service(s), so webmail providers like [Gmail](https://mail.google.com) can't snoop.  See the [SyndicateMail](https://github.com/jcnelson/syndicatemail) project for details.

Where can I learn more?
-----------------------

Please see a look at our [whitepaper](https://www.cs.princeton.edu/~jcnelson/acm-bigsystem2014.pdf), published in the proceedings of the 1st International Workshop on Software-defined Ecosystems (colocated with HPDC 2014).

Also, please see [our NSF grant](http://www.nsf.gov/awardsearch/showAward?AWD_ID=1541318&HistoricalAwards=false) for our ongoing work.

Building
--------

This process is a bit involved, and will be automated for the first release.  You should be familiar with GNU make.

**NOTE 1:**  Our build system currently honors `PREFIX` but not `DESTDIR`.  Let us know if you need the `DESTDIR` convention, and we'll add it.  `PREFIX` defaults to `/usr/local/`.

**NOTE 2:**  At this time, there are no `install` targets for the executables (this will be added soon).  For now, executables are put into directories within `./build/out/bin`.

To build Syndicate, you will need the following tools, libraries, and header files:
* [libcurl](http://curl.haxx.se/libcurl/)
* [libmicrohttpd](https://www.gnu.org/software/libmicrohttpd/)
* [Google Protocol Buffers](https://github.com/google/protobuf)
* [OpenSSL](https://www.openssl.org/)
* [libjson](https://github.com/json-c/json-c)
* [fskit](https://github.com/jcnelson/fskit)
* [libfuse](https://github.com/libfuse/)
* [Python 2.x](https://www.python.org)
* [Cython](https://github.com/cython/cython)
* [boto](https://github.com/boto/boto)

Before doing anything else, you must first build and install Syndicate's protobuf definitions, followed by `libsyndicate` and its headers.  To do so, type:

```
    $ make -C protobufs
    $ make -C libsyndicate
    $ sudo make -C libsyndicate install 
```

After this, you can build the Metadata Service.  First, edit the `./MS.mk` file and set the various fields to something other than the default values.  Then, you can build the MS with:

```
    $ make -C ms
```

Then, you must build and install the user- and gateway-facing Syndicate library (`libsyndicate-ug`).  To do so, type:

```
    $ make -C libsyndicate-ug
    $ sudo make -C libsyndicate-ug install
```

Finally, you can build the Syndicate gateways, tools, and Python package.  To do so, type:

```
   $ make -C gateways
   $ make -C python
   $ sudo make -C python install
```


Setting it up
-------------

The Metadata Service runs in Google AppEngine, or [AppScale](https://github.com/AppScale/appscale) if you prefer to run the MS on your own infrastructure.  You can run it locally with the [Python GAE development environment](https://cloud.google.com/appengine/downloads?hl=en).  Please see the relevant documentation for GAE, AppScale, and the development environment for deployment instructions.  You should be able to run the MS on the free tier in GAE.

Once you have an MS running somewhere, you'll need to make a Syndicate config file.  There is an example one [here](https://github.com/jcnelson/syndicate/tree/master/example/syndicate.conf).  You should change the various absolute paths to refer to your own build environment.  Please also consider setting the `verify_peer` field to `True` if you intend to do anything beyond testing.  You should place the configuration file into `~/.syndicate/syndicate.conf`.

The Syndicate administration tool (`/build/out/bin/syndicate`) allows you to manipulate users, volumes, and gateways.  Each method has built-in documentation that describes all required and optional positional and keyword arguments.  

This section will be fleshed out in the coming weeks.
