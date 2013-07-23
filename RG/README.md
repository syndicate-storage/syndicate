syndicate RGs
=========

Documentation for various RGs. Right now we have

- RG-s3 for Amazon S3
- RG-glacier for Amazon Glacier
- RG-gcloud for Google Cloud Storage

Note that Google Drive is different from Google Cloud Storage. Google Drive is meant to give apps access to private files of normal users, where as Google Cloud Storage is the equivalent of S3 from Google where apps can create "buckets" and use the storage as a general purpose storage.

Installation Notes for RG-gcloud (Google Cloud Storage)
-------------------------------------------------------

- Download and install Google AppEngine SDK 
https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python

- Make "Symlinks" for command-line access by clicking "Make Symlinks..." from the "GoogleAppEngineLauncher" menu. At this time, you should be able to use dev_appserver.py to run a test webserver on http://localhost:8080

- Install from gsutil.tar.gz file -- pip install gsutil (gives error), 

- Need to setup .boto config (moving this to /etc)
