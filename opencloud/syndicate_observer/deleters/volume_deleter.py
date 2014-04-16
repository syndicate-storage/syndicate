import os
import sys
import traceback

if __name__ == "__main__":
    # for testing 
    if os.getenv("OPENCLOUD_PYTHONPATH"):
        sys.path.append( os.getenv("OPENCLOUD_PYTHONPATH") )
    else:
        print >> sys.stderr, "No OPENCLOUD_PYTHONPATH variable set.  Assuming that OpenCloud is in PYTHONPATH"

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "planetstack.settings")


from syndicate.models import Volume
from observer.deleter import Deleter
from util.logger import Logger, logging
logger = Logger(level=logging.INFO)

# syndicatelib will be in steps/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class VolumeDeleter(Deleter):
        model='Volume'

        def __init__(self, **args):
            Deleter.__init__(self, **args)

        def call(self, pk, model_dict):
            try:
                volume_name = model_dict['name']
                syndicatelib.ensure_volume_absent( volume_name )
                return True
            except Exception, e:
                traceback.print_exc()
                logger.exception("Failed to erase volume '%s'" % volume_name)
                return False
            

if __name__ == "__main__":
    vd = VolumeDeleter()
    vd.call( 1, {'name': 'testvolume'} )
    
