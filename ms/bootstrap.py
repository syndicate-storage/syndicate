'''

John Whelchel
Summer 2013

Bootstraping script for PlanetLab slices that
donwloads, installs, and runs a UG on a sliver. It
authenticates to the MS and announce itself as well.


'''

import subprocess
import urllib2
import logging

VERBOSE = True
MS_URL = 'https://www.syndicate-metadata.appspot.com/syn/UG/create'

# Is this right?
REPO_NAME = 'Syndicate-nightly'
REPO_URL = 'http://vcoblitz-cmi.cs.princeton.edu/yum/Syndicate-nightly.repo' 
PACKAGE = 'syndicate-UG'
UG_COMMAND = 'syndicate-UG'


# UG settings
UG_PASSWORD = 'sniff'
UG_HOST = 'localhost'
UG_PORT = 56789
UG_RW = True
UG_NAME = 'UG' + str(UG_HOST) + str(UG_PORT)
UG_VOLUME = None

def v(text):
    if VERBOSE:
        logging.info(text)
    else return

def get_credentials():
    '''
    This method obtains SyndicateUser credentials for the running sliver.
    Will be plugged in from Jude.
    '''
    return 'jlwhelch@princeton.edu', 'Jtiger50742'

def authenticate(username, password):
    '''
    This method obtains SyndicateUser credentials for the running sliver.
    Will be plugged in from Jude.
    '''
    return True

def add_repo(name, URL):
    '''
    This method adds the UG repository to yum.
    '''
    error = ""
    out = ""

    # ********* CHECK TO SEE IF ALREADY EXTANT **********************

    # Add
    ret = subprocess.call(["yum-config-manager", "--add-repo", URL], stdout=out, stderr=error )
    v("Repo addition output:")
    v(out)
    if (ret):
        logging.error("Unable to add repo.")
    if error:
        logging.error("Repo addition error:")
        logging.error(error)
        return 1

    # Enable
    ret = subprocess.call(["yum-config-manager", "--enable", name], stdout=out, stderr=error)
    v("Repo enable output:")
    v(out)
    if (ret):
        logging.error("Unable to enable repo.")
    if error:
        logging.error("Repo enable error:")
        logging.error(error)
        return 1

    return ret

def clean_up_repo(name):
    error = ""
    out = ""

    # Disable
    ret = subprocess.call(["yum-config-manager", "--disable", name], stdout=out, stderr=error)
    v("Repo disable output:")
    v(out)
    if (ret):
        logging.error("Unable to disable repo.")
    if error:
        logging.error("Repo disable error:")
        logging.error(error)
        return 1

    return ret

def install_UG():
    '''
    This method will install the UG via Yum if not already installed.
    '''
    error = ""
    out = ""

    ret = add_repo(REPO_NAME, REPO_URL)
    if ret:
        return 1

    # Update
    ret = subprocess.call(["yum", "update"], stdout=out, stderr=error)
    v("UG update output:")
    v(out)
    if (ret):
        logging.error("Unable to update UG via yum.")
    if error:
        logging.error("UG update error:")
        logging.error(error)
        return 1 

    # *************** CHECK TO SEE IF ALREADY INSTALLED ********************

    # install
    ret = subprocess.call(["yum", "install", PACKAGE], stdout=out, stderr=error)
    v("UG install output:")
    v(out)
    if (ret):
        logging.error("Unable to install UG via yum.")
    if error:
        logging.error("UG install error:")
        logging.error(error)
        return 1

    clean_up_repo(REPO_NAME)

    return ret

def run_UG():
    '''
    This method will run the UG
    '''
    error = ""
    out = ""
    proc = subprocess.popen(UG_COMMAND, stdout=out, stderr=error)
    v("UG run output:")
    v(out)
    if not proc:
        logging.error("Unable to run repo.")
    if error:
        logging.error("UG run error:")
        logging.error(error)
        return 1


    return proc

def notify_MS(params):
    
    data = urllib2.urlencode(params)
    response = urllib2.urlopen(MS_URL, params)
    if "error" in response:
        raise Exception("Unable to notify MS of new UG.")

def bootstrap():
    '''
    This is the main bootstrap method
    '''

    # Get credentials and authenticate
    username, password = get_credentials()
    if not authenticate(username, password):
        raise Exception("Unable to authenticate with given credentials for user {}".format(username))

    # Install
    if install_UG():
        # Error while installing
        raise Exception("Unable to install UG via yum.")

    # Run once
    proc = run_UG
    if proc is None:
        raise Exception("Unable to run UG for the first time.")
    running = 1

    # Alert MS
    if UG_VOLUME:
        params = {'g_name':UG_NAME,
                  'g_password':UG_PASSWORD,
                  'host':UG_HOST,
                  'port':UG_PORT,
                  'read_write':UG_RW,
                  'volume_name':UG_VOLUME
                  }
    else:
        params = {'g_name':UG_NAME,
                  'g_password':UG_PASSWORD,
                  'host':UG_HOST,
                  'port':UG_PORT,
                  'read_write':UG_RW
                  }
    notify_MS(params)

    # Keep running
    while True:
        if not running:
            proc = run_UG()
            if proc is None:
                raise Exception("Unable to keep UG running.")
        proc.poll()
        running = (proc.returncode is None) # returncode is None when process has not terminated yet.


if __name__ == "__main__":
    try:
        bootstrap()
    except Exception as e:
        print "Unable to bootstrap local Syndicate User Gateway."
        print "Received error: {}".format(e)
        print "Exiting."
        exit(1)