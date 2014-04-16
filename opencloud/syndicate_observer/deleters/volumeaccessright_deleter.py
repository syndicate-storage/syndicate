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


from util.logger import Logger, logging
logger = Logger(level=logging.INFO)

from syndicate.models import VolumeAccessRight
from observer.deleter import Deleter

# hpclibrary will be in steps/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class VolumeAccessRightDeleter(Deleter):
        model='VolumeAccessRight'

        def __init__(self, **args):
            Deleter.__init__(self, **args)

        def call(self, pk, model_dict):
            print "XXX delete volume access right", model_dict


if __name__ == "__main__":
    vard = VolumeAccessRightDeleter()
