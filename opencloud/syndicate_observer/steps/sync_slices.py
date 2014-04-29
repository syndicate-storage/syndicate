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
from core.models import Slice, User
from syndicate.models import Volume, VolumeSlice

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

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib
import openidlib

class SyncSlices(SyncStep):
    provides=[Slice]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        try:
            ret = Slice.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))
            return ret
        except Exception, e:
            traceback.print_exc()
            return None

    def sync_record(self, slice):
        """
        Synchronize a Syndicate Slice record with Syndicate.
        """

        print "Sync - per-slice volume!"
        print "slice = %s" % slice.name

        # we now pass the base name to OpenID, because the OpenID server only supports
        # user names up to 30 characters.

        slice_email = slice.name
        slice_password = syndicatelib.registration_password()
        print "slice email = %s" % slice_email

        # create the slice-openid
        logger.info('Create per-slice openID. Slice = %s, openid = %s' % (slice.name, slice_email))
        try:
            new_user = openidlib.createOrUpdate_user( slice_email, slice_password )
        except Exception, e:
            traceback.print_exc()
            logger.error("Creation of per-slice openID was failed")
            raise e

        logger.info('Create per-slice syndicate volume. Slice = %s, Slice-openid = %s' % (slice.name, slice_email))
        new_volume = Volume()
        new_volume.name = slice.name
        new_volume.owner_id = slice.creator
        new_volume.blocksize = 1024000
        new_volume.private = True
        new_volume.archive = False
        new_volume.default_gateway_caps = Volume.CAP_READ_DATA | Volume.CAP_WRITE_DATA | Volume.CAP_HOST_DATA
        new_volume.save()

        new_volumeslice = VolumeSlice()
        new_volumeslice.volume_id = new_volume
        new_volumeslice.slice_id = slice
        new_volumeslice.gateway_caps = Volume.CAP_READ_DATA | Volume.CAP_WRITE_DATA | Volume.CAP_HOST_DATA
        new_volumeslice.peer_portnum = 32780
        new_volumeslice.replicate_portnum = 32781
        new_volumeslice.save()
        
        return True


if __name__ == "__main__":
    ss = SyncSlices()

    recs = ss.fetch_pending()

    for rec in recs:
        ss.sync_record( rec )
