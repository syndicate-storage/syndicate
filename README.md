Syndicate
=========

Syndicate is the virtual private cloud storage system that scales.  You pick any combination of local disk, cloud storage, and network caches, and use Syndicate to turn them into an always-on, scalable read/write storage medium tailored to you and your application's needs.  Syndicate keeps your data private to you and the people you authorize, and employs strong cryptography to prevent anyone else from accessing it.

Core Features
-------------

Syndicate is *decentralized*.  You can distribute your Syndicate instance across multiple clouds, multiple local networks, and multiple devices.  Syndicate is not tied to any specific provider.

Syndicate is *secure*.  Syndicate implements a trusted cloud storage layer, forming a narrow waist between you and your storage.  Syndicate components are mutually distrustful, and rely on proven cryptography to authenticate to you and to one another.

Syndicate is *extensible*.  You can control how Syndicate manages file data.  Syndicate lets you deploy storage-level policies in the form of simple Python scripts which let you define exactly what Syndicate does with your data.

Syndicate is *scalable*.  You can use Syndicate to share data across just your personal devices, or you can use it to distribute terabytes of data to millions of people.  All you have to do is give it enough cloud storage and network caches to do so.

Syndicate is *consistent*.  Even though it uses network caches to help deliver your data to readers, it employs a novel consistency protocol to ensure that your readers do not hit stale cache data.  In doing so, it lets you leverage unmodified caching proxies and CDNs without having to rely on them to honor your HTTP cache control directives.


Building
--------

To build Syndicate on your local system, you'll first need to install the packages listed in the INSTALL file, as well as the Google AppEngine Python SDK.  Then, issue the following commands:

```
$ scons libsyndicate
$ sudo scons libsyndicate-install
$ scons ms
$ scons replica_manager
$ scons UG
```

