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

Alternatively, you can get nightly RPMs from our [build server](http://vcoblitz-cmi.cs.princeton.edu/syndicate-nightly/RPMS/).  They're compiled for Fedora 12, since that's what we run on [PlanetLab](http://www.planet-lab.org).


How-To
------

### Overview ###

Syndicate organizes your data into one or more Volumes.  A Volume is a logical collection of data organized into a filesystem.  Syndicate's components work together to implement your Volumes on top of your cloud storage and caches.

There are four major components of Syndicate:  the Metadata Service ([MS](https://github.com/jcnelson/syndicate/tree/master/ms)), the User Gateway ([UG](https://github.com/jcnelson/syndicate/tree/master/UG)), the Replica Gateway ([RG](https://github.com/jcnelson/syndicate/tree/master/RG)), and the Acquisition Gateway ([AG](https://github.com/jcnelson/syndicate/tree/master/AG)).  The MS is an always-on service that Gateways use to coordinate--in many ways, it's similar to a private BitTorrent tracker.  The UGs are peers that cache and exchange Volume data and metadata on your behalf, and give you read/write interfaces to your data.  For example, there is a FUSE UG, and a Web store UG.  RGs are processes that take data from UGs and back it up to your storage, so other UGs can get to it.  AGs are processes that expose external, existing datasets as read-only directory hierarchies in your Volumes.

At a minimum, you need to deploy the MS, one UG, and one RG.  That will get you Dropbox-like functionality.

### Metadata Service ###

Before you do anything, you (or someone you trust) will first need to get an MS up and running.  If you're going to be using an existing MS, you can skip this section.

You can deploy the MS in one of two ways: as a [Google AppEngine](https://developers.google.com/appengine/) Python app, or as an [AppScale](http://www.appscale.com) Python app.  You can also run it locally (i.e. in a test environment) with the [Google AppEngine SDK](https://developers.google.com/appengine/docs/python/tools/devserver).

Before you deploy your MS, you will need to set up an administrator account.  To do so, you'll need an RSA 4096-bit key.  You can generate an RSA 4096-bit key pair with these commands:

```
$ openssl genrsa -out /path/to/your/admin/key.pem 4096
$ openssl rsa -in /path/to/your/admin/key.pem -pubout > /path/to/your/admin/public/key.pub
```

Then, you can set up the administrator account with this command:

```
$ scons MS-setup-admin email=YOUR.EMAIL@EXAMPLE.COM key=/path/to/your/admin/public/key.pub
```

You'll also need to generate an app.yaml file.  The only thing Syndicate needs from you is the MS name to use.  To generate the file, run this command:

```
$ scons MS-setup-app name=YOUR-APP-NAME
```

Now you can deploy the MS.  For example, to deploy to Google AppEngine, you simply use Google's appcfg.py script to upload the MS you just built to the AppEngine PaaS:

```
$ appcfg.py update build/out/ms
```

Once the MS is up and running, you need to set up the Syndicate management tool syntool.py, so you can go on to create users, Volumes, and Gateways.  You'll need to give it your administrator's email address and public key, as well as the URL to the MS's API (usually, this is the MS's hostname, followed by /api).  To do so, simply run:

```
$ syntool.py --user_id YOUR.EMAIL@EXAMPLE.COM --MSAPI https://YOUR.RUNNING.MS/api --privkey /path/to/your/admin/key.pem setup
```

You can remove the private key you generated after this step, since syntool.py makes a copy of it and puts it into its configuration directory (by default, this is $HOME/.syndicate/).

From start to finish, here's a simple recipe that builds, sets up, and deploys the MS on Google AppEngine.

```
$ openssl genrsa -out ~/syndicate_admin.pem 4096
$ openssl rsa -in ~/syndicate_admin.pem -pubout > ~/syndicate_admin.pub
$ scons MS
$ scons MS-setup-admin email=admin@syndicatedrive.com key=~/syndicate_admin.pub
$ scons MS-setup-app name=syndicate-drive-ms
$ appcfg.py update build/out/ms
$ syntool.py --user_id admin@syndicatedrive.com --MSAPI https://syndicate-drive-ms.appspot.com/api --privkey ~/syndicate_admin.pem setup
$ rm ~/syndicate_admin.pem
```

### Volumes ###

Before you create your first Volume, make sure you have set up syntool.py with the Syndicate user ID you want to run as (this is the email address that identifies your user account on the MS you use).  This can be done with:

```
$ syntool.py --user_id YOUR.EMAIL@EXAMPLE.COm --MSAPI https://YOUR.RUNNING.MS/api --privkey /path/to/your/private/key.pem setup
```

Once syntool.py is set up, you can use it to create and manage your Volumes.  To create a Volume called "HelloWorld" with a block size of 60KB, simply issue the following command:

```
$ syntool.py create_volume YOUR.EMAIL@EXAMPLE.COM HelloWorld "This is my first Volume, called HelloWorld" 61440 default_gateway_caps=ALL
```

You can see a listing of what each positional and keyword argument means with:

```
$ syntool.py help create_volume
```


### User Gateways ###

UGs do two things: act as Syndicate clients, and coordinate with one another to cache and distribute written data.  There are two UGs: a FUSE filesystem (syndicatefs), and a Web object store (syndicate-httpd).

