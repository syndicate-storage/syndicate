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
from core.models import Service, Slice

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

from syndicate_storage.models import VolumeSlice,VolumeAccessRight,Volume

import syndicate.observer.sync as syndicatelib


class SyncVolumeSlice(SyncStep):
    provides=[VolumeSlice]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        return VolumeSlice.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))

    def sync_record(self, vs):
        return syndicatelib.sync_volumeslice_record(vs)


if __name__ == "__main__":
    sv = SyncVolumeSlice()

    # first, set all VolumeSlice to not-enacted so we can test 
    for v in VolumeSlice.objects.all():
       v.enacted = None
       v.save()

    # NOTE: for resetting only 
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
       sys.exit(0)

    recs = sv.fetch_pending()

    for rec in recs:
        if rec.slice_id.creator is None:
           print "Ignoring slice %s, since it has no creator" % (rec.slice_id)
           continue

        sv.sync_record( rec )

