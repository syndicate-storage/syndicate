Syndicate
=========

Syndicate is a scalable software-defined storage system.  It gives users the ability to manage data consistency guarantees, dataflow processing logic, and administrative and trust boundaries in an end-to-end fashion, across a dynamic set of existing services.  By doing so, Syndicate can safely combine multiple existing cloud services into a coherent storage medium, and remain resilient to individual service outages, API changes, and cost changes.  Applications using Syndicate do not need to evolve in lock-step with the cloud services they leverage, and application developers can mix and match services to find their cost/performance sweet spots.

A Syndicate deployment is comprised of **users**, **volumes**, and **gateways**:
* A user represents an administrative domain, and incorporates the notion of a set of hosts under the control of a single principal.  For example, you are the user of your personal devices, while Google is the user of their datacenters.
* A volume is not only a collection of files and directories, but also represents a trust domain.  Volumes intersect one or more administrative domains, and incorporate the set of hosts that process I/O requests on its data.  Additionally, a volume defines what kinds of I/O requests may occur at each intersection point.
* A gateway is a service at the intersection between a user and a volume, which processes I/O requests.  It can be as small as a single process on a single host, or as large as a fleet of datacenters.  A user instantiates one or more gateways in a volume to access its data, and a user defines exactly how the gateway handles I/O requests from other gateways.  There are three gateway flavors:
   * **User Gateways**:  These are gateways that interact with the user's applications.  User gateways coordinate to maintain each file's consistency and control how file data flows throughout the system.
   * **Replica Gateways**:  These are gateways that take data from user gateways, and make it persistent.  They serve it back upon request.  They don't coordinate I/O, but instead operate on immutable, globally-unique chunks of data.
   * **Acquisition Gateways**:  These are gateways that map data from external sources as files and directories within the volume.  They are read-only.

Gateways discover one another and bootstrap coordination via an untrusted PaaS-hosted Metadata Service.  The current implementation is compatible with Google AppEngine and AppScale.

The code for Syndicate is split into individual repositories under the [syndicate-storage](https://github.com/syndicate-storage) organization.  The main Syndicate components are:

* [Syndicate Core](https://github.com/syndicate-storage/syndicate-core):  Contains `libsyndicate`, `libsyndicate-ug`, the Syndicate Metadata Service, the Syndicate Python package (including cloud service drivers), and all the protocol definitions.
* [Syndicate UG tools](https://github.com/syndicate-storage/syndicate-ug-tools):  Contains a set of command-line client tools that interact with Syndicate files and directories.
* [Syndicatefs](https://github.com/syndicate-storage/syndicatefs):  FUSE filesystem client to Syndicate volumes.
* [Syndicate RG](https://github.com/syndicate-storage/syndicate-rg):  Syndicate replica gateway--a gateway that persists data on behalf of other gateways and serves it back on request.
* [Syndicate AG](https://github.com/syndicate-storage/syndicate-ag):  Syndicate acquisition gateway--a gateway that exposes data in existing services as files and directories in a Syndicate volume.
* [Syndicate Automount](https://github.com/syndicate-storage/syndicate-automount):  Syndicate automount service--a network service for provisioning and managing Syndicate gateways at scale.

Building
========

The code in this repository is legacy, and preserved here for posterity.  Do not use it; use the code in the individual repositories above.
