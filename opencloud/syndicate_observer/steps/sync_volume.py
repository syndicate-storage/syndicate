#!/usr/bin/python

import os
import sys
import traceback
import base64

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
from syndicate.models import Volume
from util.logger import Logger, logging
logger = Logger(level=logging.INFO)

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib


class SyncVolume(SyncStep):
    provides=[Volume]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        try:
            ret = Volume.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))
            return ret
        except Exception, e:
            traceback.print_exc()
            return None

    def sync_record(self, volume):
        try:
            print "Sync!"
            print "volume = %s" % volume.name
        
            # owner must exist...
            try:
                new_user = syndicatelib.ensure_user_exists( volume.owner_id.email )
            except Exception, e:
                traceback.print_exc()
                logger.error("Failed to ensure user '%s' exists" % volume.owner_id.email )
                return False

            # volume must exist 
            try:
                syndicatelib.ensure_volume_exists( volume.owner_id.email, volume, user=new_user )
            except Exception, e:
                traceback.print_exc()
                logger.error("Failed to ensure volume '%s' exists" % volume.name )
                return False

            return True

        except Exception, e:
            traceback.print_exc()
            return False
        



if __name__ == "__main__":
    sv = SyncVolume()

    recs = sv.fetch_pending()

    for rec in recs:
        sv.sync_record( rec )
