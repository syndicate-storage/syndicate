Syndicate
=========

Syndicate is the virtual private cloud storage system that scales.  You pick any combination of local storage, cloud storage, CDNs, and HTTP caches, and use Syndicate to turn them into an always-on, scalable read/write storage system tailored to your application's needs.

Why use Syndicate?
------------------

Syndicate is **decentralized**.  You can distribute Syndicate across multiple clouds, multiple local networks, and multiple devices.  Syndicate is not tied to any specific provider.

Syndicate is **scalable**.  You can use Syndicate to share a few files across just your personal devices, or you can use it to distribute terabytes of data to millions of people.

Syndicate is **flexible**.  It doesn't matter what underlying storage and caches you give to Syndicate; Syndicate will combine them all into a single, virtual cloud that looks and acts the same regardless of your choices.  Syndicate offers a simple but powerful driver model that lets you add support for more back-ends and define file-specific storage policies with minimal effort.

Syndicate is **private**.  Syndicate gives you automatic end-to-end encryption on a per-file basis, such that only your endpoints can see the data.  The underlying cloud storage and caches only see ciphertext.

Syndicate is **secure**.  Syndicate prevents unauthorized access and components rely on strong, proven cryptography to authenticate to you and to one another.  It is tamper-resistent and lets you quickly add and revoke access to your data on a per-user, per-host, and per-file basis.


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

