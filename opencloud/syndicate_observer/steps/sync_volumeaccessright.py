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
from util.logger import Logger, logging
logger = Logger(level=logging.INFO)

# point to planetstack 
if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
    sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
else:
    logger.warning("No OPENCLOUD_PYTHONPATH set; assuming your PYTHONPATH works") 

from syndicate.models import VolumeAccessRight

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
        
        try:
            # get arguments
            config = syndicatelib.get_config()
            user_email = vac.owner_id.email
            volume_name = vac.volume.name
            syndicate_caps = syndicatelib.opencloud_caps_to_syndicate_caps( vac.gateway_caps ) 
            
            # validate config
            try:
               RG_port = config.SYNDICATE_RG_DEFAULT_PORT
            except Exception, e:
               logger.print_exc()
               logger.error("syndicatelib config is missing SYNDICATE_RG_DEFAULT_PORT")
               return False
            
            print "Sync Volume Access Right!"
            print "Sync for (%s, %s)" % (user_email, volume_name)
            
            # ensure the user exists and has credentials
            try:
                new_user = syndicatelib.ensure_user_exists_and_has_credentials( user_email )
            except Exception, e:
                traceback.print_exc()
                logger.error("Failed to ensure user '%s' exists" % user_email )
                return False

            # make the access right for the user to create their own UGs, and provision an RG for this user that will listen on localhost.
            # the user will have to supply their own RG closure.
            try:
                rc = syndicatelib.setup_volume_access( user_email, volume_name, syndicate_caps, RG_port, observer_secret )
            except Exception, e:
                traceback.print_exc()
                logger.error("Faoed to ensure user %s can access Volume %s with rights %s" % (user_email, volume_name, syndicate_caps))
                return False

            return rc
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to ensure user %s can access Volume %s with rights %s" % (user_email, volume_name, syndicate_caps))
            return False


if __name__ == "__main__":
    sv = SyncVolumeAccessRight()

    recs = sv.fetch_pending()

    for rec in recs:
        sv.sync_record( rec )
