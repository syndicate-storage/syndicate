#!/bin/sh

# automatic Syndicate upgrades 

aptitude update 
aptitude -y upgrade libsyndicate libsyndicate-ug syndicate-ug syndicate-rg syndicate-ag python-syndicate syndicated-opencloud syndicate-opencloud syndicate-ms-clients
