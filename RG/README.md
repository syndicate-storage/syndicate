Syndicate Replica Manager (and Storage Drivers)
=========

Documentation for various RGs. Right now we have

- RG-s3 for Amazon S3
- RG-glacier for Amazon Glacier
- RG-gcloud for Google Cloud Storage

Note that Google Drive is different from Google Cloud Storage. Google Drive is meant to give apps access to private files of normal users, where as Google Cloud Storage is the equivalent of S3 from Google where apps can create "buckets" and use the storage as a general purpose storage.

Installation Notes for sd_gcloud (Google Cloud Storage)
-------------------------------------------------------

- Download and install Google AppEngine SDK 
https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python

- Make "Symlinks" for command-line access by clicking "Make Symlinks..." from the "GoogleAppEngineLauncher" menu. At this time, you should be able to use dev_appserver.py to run a test webserver on http://localhost:8080

- Install from gsutil.tar.gz file -- NOTE: "pip install gsutil" runs into errors 

- Need to setup .boto config (moving this to /etc)


Installation Notes for sd_dropbox (Dropbox)
-------------------------------------------------------

- Download and install Dropbox python SDK 
https://www.dropbox.com/developers/core/sdks/python -- NOTE: "pip install dropbox" works just fine

- Register for a new app on Dropbox
https://www.dropbox.com/developers/apply?cont=/developers/apps

- For first time use, get the ACCESS_TOKEN
"./sd_dropbox.py --access" make sure that DROPBOX_APP_KEY and DROPBOX_APP_SECRET are set in etc/config.py file

Installation Notes for sd_box (Box.net -- now Box.com)
-------------------------------------------------------