#!/usr/bin/python

import os
import sys
import base64
import traceback

if __name__ == "__main__":
    # for testing 
    if os.getenv("OPENCLOUD_PYTHONPATH"):
        sys.path.append( os.getenv("OPENCLOUD_PYTHONPATH") )
    else:
        print >> sys.stderr, "No OPENCLOUD_PYTHONPATH variable set.  Assuming that OpenCloud is in PYTHONPATH"
 
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")

from django.db.models import F, Q
from planetstack.config import Config
from observer.syncstep import SyncStep
from core.models import Service

import logging
from logging import Logger
logging.basicConfig( format='[%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )
logger = logging.getLogger()
logger.setLevel( logging.INFO )

# point to planetstack 
if __name__ != "__main__":
    if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
        sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
    else:
        logger.warning("No OPENCLOUD_PYTHONPATH set; assuming your PYTHONPATH works")

from syndicate_storage.models import VolumeAccessRight

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class SyncVolumeAccessRight(SyncStep):
    provides=[VolumeAccessRight]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        return VolumeAccessRight.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))

    def sync_record(self, vac):
        
        syndicate_caps = "UNKNOWN"  # for exception handling
        
        # get arguments
        config = syndicatelib.get_config()
        user_email = vac.owner_id.email
        volume_name = vac.volume.name
        syndicate_caps = syndicatelib.opencloud_caps_to_syndicate_caps( vac.cap_read_data, vac.cap_write_data, vac.cap_host_data ) 
        
        logger.info( "Sync VolumeAccessRight for (%s, %s)" % (user_email, volume_name) )
        
        # validate config
        try:
           RG_port = config.SYNDICATE_RG_DEFAULT_PORT
           observer_secret = config.SYNDICATE_OPENCLOUD_SECRET
        except Exception, e:
           traceback.print_exc()
           logger.error("syndicatelib config is missing SYNDICATE_RG_DEFAULT_PORT, SYNDICATE_OPENCLOUD_SECRET")
           raise e
            
        # ensure the user exists and has credentials
        try:
            rc, user = syndicatelib.ensure_principal_exists( user_email, observer_secret, is_admin=False, max_UGs=1100, max_RGs=1 )
            assert rc is True, "Failed to ensure principal %s exists (rc = %s,%s)" % (user_email, rc, user)
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to ensure user '%s' exists" % user_email )
            raise e
 
        # make the access right for the user to create their own UGs, and provision an RG for this user that will listen on localhost.
        # the user will have to supply their own RG closure.
        try:
            rc = syndicatelib.setup_volume_access( user_email, volume_name, syndicate_caps, RG_port, observer_secret )
            assert rc is True, "Failed to setup volume access for %s in %s" % (user_email, volume_name)

        except Exception, e:
            traceback.print_exc()
            logger.error("Faoed to ensure user %s can access Volume %s with rights %s" % (user_email, volume_name, syndicate_caps))
            raise e

        return True


if __name__ == "__main__":

    # first, set all VolumeAccessRights to not-enacted so we can test 
    for v in VolumeAccessRight.objects.all():
       v.enacted = None
       v.save()

    # NOTE: for resetting only 
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
       sys.exit(0)


    sv = SyncVolumeAccessRight()
    recs = sv.fetch_pending()

    for rec in recs:
        sv.sync_record( rec )

