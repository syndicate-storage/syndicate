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
from syndicate_storage.models import Volume

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


import syndicate.observer.sync as syndicate_observer_sync 


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

    def fetch_deleted(self):
        try:
            ret = Volume.deleted_objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))
            return ret
        except Exception, e:
            traceback.print_exc()
            return None


    def sync_record(self, volume):
        """
        Synchronize a Volume record with Syndicate.
        """
        return syndicate_observer_sync.sync_volume_record( volume )
        
    def delete_record(self, volume):
        return syndicate_observer_sync.delete_volume_record( volume )
        
if __name__ == "__main__":
    sv = SyncVolume()


    # first, set all volumes to not-enacted so we can test 
    for v in Volume.objects.all():
       v.enacted = None
       v.save()
    
    # NOTE: for resetting only 
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
       sys.exit(0)

    recs = sv.fetch_pending()

    for rec in recs:
        rc = sv.sync_record( rec )
        if not rc:
          print "\n\nFailed to sync %s\n\n" % (rec.name)

    
    delrecs = sv.fetch_deleted()

    for rec in delrecs:
       sv.delete_record( rec )
    