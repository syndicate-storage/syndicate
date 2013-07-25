Name: Syndicate Metadata-Service (MS)

Authors: Jude Nelson and John Whelchel

Readme last updated: 7/9/13 23:00 PM

The files in syndicate/ms run the Syndicate meta-data service. The overall
application is a combination of the frameworks webapp2 and django, running
on Google App Engine. All URLs directly following the host name are run
on webapp2 handlers and are really used for communication between the MS and
gateways. All URLs preceded by /syn are manual Syndicate Administration tools.

For those reading this for the first time, the most relevant sections are
LOCAL BASICs, NON-GUI URIs and MS_RECORD_DETAILS



REQUIREMENTS:

	All packages for syndicate as per /syndicate/INSTALL as well as the Google
	App Engine Python SDK.

LOCAL BASICS:

	Make sure libsyndicate has been built per /syndicate/INSTALL. Then make sure
	the MS is most up to date by running 'scons ms' in the main directory. To
	start the MS locally, run the GAE command 

		$ dev_appserver(*) /build/out/ms

	The MS admin tool and service runs on localhost, port 8080, while the GAE 
	administration tool allowing manual viewing of the datastore, cache clearing,
	and manual creation of entities is on port 8000. The MS GUI requires a
	PlanetLab/Vicci account. After logging in, you can manually create volumes,
	User Gateways, Replica Gateways, and Acquisition Gateways, as well as modify
	some of their settings. 

	(*)useful parameter for dev_appserver 'clear-datastore=yes'

	N.B. Currently, the actual parameters for MS records, especially for gateways,
	are in flux. Keep that in mind when creating new ones.

UPDATING LIVE:

	To update the site (syndicate-metadata.appspot.com), run 
	 	$ appcfg.py update build/out/ms
	It requires a user email and password, which you should get from Jude (I feel 
	I shouldn't post it here).


CONFIGURATION/SETTINGS:
	
	The overall configuration of the MS is in app.yaml (a GAE file read when
	loading the MS). The most important part are handlers, which map URLS
	to either webapp2, static files, or django, and the built-ins which
	dictate what GAE-packaged libraries should be used. GAE middleware is
	run in appengine_config.py. Regular cron jobs (specifically clearing old
	sessions) are configured in cron.yaml and run in cron.py.

	Django configuration is in django_syndicate_web/settings.py. Main django URL 
	mappings are in django_syndicate_web/urls.py. Main webapp2 handlers are in 
	MS/handlers.py.



NON-GUI URIS:

    The MS currently is divided into two parts: 1) a django-based syndicate
    admin GUI tool, and a pure URI webapp2 communication tool. This section
    explains the functionality of the non-GUI URIS (as can be found in ms/msapp.py).

    DEBUG

    The debug URI path allows running tests from ms/tests. An example URI would be
	/debug/setup/test?do_init=1&ug_name=UG-localhost&do_local_ug=1&username=jlwhelch@
	princeton.edu

	The name of the test desired is the parameter after debug (options are in /tests).
	Other paramters are given in the ampersand seperated list after the required clause
	"/test?..."

		TESTS:

			setup(**):
				Parameter list (& seperation):

                'start' and 'end'
                # change the range of UGs to create via nodes at top of setup.py

                do_ugs
                # create UGs from 'start' to 'end'

                do_rgs
                # create UGs from 'start' to 'end'

                do_ags
                # create UGs from 'start' to 'end'

                do_init (**)(recommended for all usages of 'setup')
                # create starter volumes and users

                do_local_ug
                # only create a UG for localhost 

                do_local_rg
                # create an RG for localhost in MS records

                do_local_ag
                # create an AG for localhost in MS records

                reset_volume
                # delete all MSEntry records for a volume, and recreate the root.


                Manual creation/deletion of UG's. All required.
                Web GUI is probably easier...
                {
                    username

                    ug_name

                    ug_action
                    # 'create' or 'dlete'

                    ug_host

                    ug_port
                }


    VOLUME

    The VOLUME URI path allows requesting meta-data for a volume from the MS. Requires
    both volume password and UG authentication HTTP headers. See 
    handlers.MSVolumeRequestHandler for more info. The volume ID is the next level of
    the URI (i.e. /VOLUME/<volume_id>)

    FILE

    The FILE URI path will read and list metadata entries via GET, and
    create, delete, and update metadata entries via POST. The URI format
    is /FILE/<volume_id>/<path>

    Look at handlers.MSFileRequestHandler for details on headers.




MS RECORD DETAILS:

	All records are stored in MS/volume.py or MS/gateway.py. Operations on the
	GAE datastore are abstracted in the storage/ directory, specifically storage/
	storage. For more information on the inner-workings, look in storage/storagetypes.py.


Syndicate Users:

	A Syndicate User (User) is automatically created upon login to the MS. When
	creating gateways and volumes, they become attached to the user creating them (
	except for RG's and AG's). Users are identified by email address.



Volumes:

    Volumes can be created and viewed in the volume subsection of the MS admin
    website. Then can be made private, in which case they are only viewable
    to you on "syn/volume/myvolumes". They must be activated after creation
    in volume settings. You can change the description and permissions on them 
    as well. Changing permissions requires knowledge of the User email for whom
    you would like to grant read or read/write permissions.



User Gateways:

    User Gateways can be created in the UG subsection of the site, as well as
    through pure URIs. The pure URI format requires a previous manual login
    however. The format (subject to change based on parameters * see NB above)
    is:

    /syn/UG/create/<volume_id>/<g_name>/<g_password>/<host>/<port>/<read_write>.


    GATEWAY GENERIC

    - volume_id is the volume to which the UG will be attached. It must already
    exist.
    - g_name represents the gateway name by which the MS will identify the
    UG. 
    - g_password is the password by which UG's will authenticate themselves
    to the MS and other gateways. 
    - Host and port are the location of the UG
    so the MS can find it later. 

    UG SPECIFIC 

    - Read_write is a boolean value dictating whether the UG is a pure read
    gateway (False) or a gateway that also has the right to write data (True).

    The GUI is comparable, but simpler (and allows spaces). When using URI,
    use '_' for spaces. Internally the MS represents all spaces that way anyway,
    and prints them as if they are ' '.


Acquistion/Replica Gateways:

    These gateways currently only have a GUI creation tool. Their parameters
    are most in flux right now. A tool will be added to upload JSON config
    files. RG's have the extra 'private' boolean parameter, meaning
    that it can only be bound to a Volume owned by the same user that owns the RG.
    This functionality has not been quite implemented yet, although the parameter
    exists. They have the same generic parameters as UG's above.
