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
            print "Sync Volume Access Right!"
            print "Sync for (%s, %s)" % (vac.owner_id.email, vac.volume.name)

            # ensure the user exists...
            try:
                new_user = syndicatelib.ensure_user_exists( vac.owner_id.email )
            except Exception, e:
                traceback.print_exc()
                logger.error("Failed to ensure user '%s' exists" % vac.owner_id.email )
                return False

            # convert caps to Syndicate caps
            syndicate_caps = syndicatelib.opencloud_caps_to_syndicate_caps( vac.gateway_caps )
            
            # make the access right 
            try:
                rc = syndicatelib.ensure_volume_access_right_exists( vac.owner_id.email, vac.volume.name, syndicate_caps )
            except Exception, e:
                traceback.print_exc()
                logger.error("Faoed to ensure user %s can access Volume %s with rights %s" % (vac.owner_id.email, vac.volume.name, syndicate_caps))
                return False

            return rc
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to ensure user %s can access Volume %s with rights %s" % (vac.owner_id.email, vac.volume.name, syndicate_caps))
            return False


if __name__ == "__main__":
    sv = SyncVolumeAccessRight()

    recs = sv.fetch_pending()

    for rec in recs:
        sv.sync_record( rec )
