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

import syndicate.observer.sync as syndicate_observer_sync

class SyncVolumeAccessRight(SyncStep):
    provides=[VolumeAccessRight]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        return VolumeAccessRight.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))

    def fetch_deleted(self):
        return VolumeAccessRight.deleted_objects.filter(Q(enacted__lt=F("updated")) | Q(enacted=None)).filter( Q(deleted=True) )
    
    def sync_record(self, vac):
        """
        Synchronize a volume access right.
        """
        return syndicate_observer_sync.sync_volumeaccessright_record( vac )
     
    def delete_record(self, vac):
        return syndicate_observer_sync.delete_volumeaccessright_record( vac )


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

    
    delrecs = sv.fetch_deleted()

    for rec in delrecs:
       sv.delete_record( rec )
