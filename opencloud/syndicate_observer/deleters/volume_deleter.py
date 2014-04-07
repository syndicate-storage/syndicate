import os
import sys
from syndicate.models import Volume
from observer.deleter import Deleter

# hpclibrary will be in steps/..
parentdir = os.path.join(os.path.dirname(__file__),"..")
sys.path.insert(0,parentdir)

import syndicatelib

class VolumeDeleter(Deleter):
        model='Volume'

        def __init__(self, **args):
            Deleter.__init__(self, **args)

        def call(self, pk, model_dict):
            print "XXX delete volume", model_dict

