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
from util.logger import Logger, logging
logger = Logger(level=logging.INFO)

# point to planetstack 
if os.getenv("OPENCLOUD_PYTHONPATH") is not None:
    sys.path.insert(0, os.getenv("OPENCLOUD_PYTHONPATH"))
else:
    logger.warning("No OPENCLOUD_PYTHONPATH set; assuming your PYTHONPATH works") 

from syndicate.models import VolumeSlice

# syndicatelib will be in stes/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib


class SyncVolumeSlice(SyncStep):
    provides=[VolumeSlice]
    requested_interval=0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        return VolumeSlice.objects.filter(Q(enacted__lt=F('updated')) | Q(enacted=None))

    def sync_record(self, vs):
        try:
            print "Mount %s on %s's slivers" % (vs.volume_id.name, vs.volume_id.name)
            
            # get the slice owner... 
            slice_owner_email = Slice.creator.email
            
            # create a Syndicate user account...
            # XXX username = syndicatelib.
            try:
                new_user = syndicatelib.ensure_user_exists( slice_owner_email )
            except Exception, e:
                traceback.print_exc()
                logger.error('Failed to ensure user %s exists' % slice_owner_email)
                return False
            
            # ensure the slice owner's access right to this Volume exists
            # NOTE: the volume should already exist, so no need to check here
            # (this method will raise an exception if it doesn't exist anyway)
            syndicate_caps = syndicatelib.opencloud_caps_to_syndicate_caps( vs.volume_id.gateway_caps )

            try:
                vac = syndicatelib.ensure_volume_access_right_exists( slice_owner_email, vs.volume_id.name, syndicate_caps )
            except Exception, e:
                traceback.print_exc()
                logger.error('Failed to grant capabilities for user %s (owner of slice %s) in volume %s' % (slice_owner_email, vs.slice_id.name, vs.volume_id.name))
                return False

            # get our private key 
            config = syndicatelib.get_config()

            pkey = syndicatelib.load_private_key( config.SYNDICATE_PRIVATE_KEY )
            if pkey is None:
                # failed to load
                logger.error("Failed to load private key from %s" % config.SYNDICATE_PRIVATE_KEY )
                return False
            
            # generate a credentials blob and store it 
            try:
                # XXX
                cred = syndicatelib.create_credential_blob( pkey.exportKey(), config.SYNDICATE_OPENCLOUD_SECRET, 
                                                            config.SYNDICATE_SMI_URL,
                                                            vs.volume_id.name,
                                                            slice_owner_email,
                                                            

            # TODO: push the Volume credentials to all of the slice's slivers

            return True
        except Exception, e:
            traceback.print_exc()
            logger.error("Failed to mount volume %s on slice %s" % (vs.volume_id.name, vs.slice_id.name))
            return False


if __name__ == "__main__":
    sv = SyncVolumeSlice()

    recs = sv.fetch_pending()

    for rec in recs:
        sv.sync_record( rec )
