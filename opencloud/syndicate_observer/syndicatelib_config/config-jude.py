#!/usr/bin/python

# configuration for syndicatelib
SYNDICATE_SMI_URL="http://localhost:8080"

SYNDICATE_OPENID_TRUSTROOT="http://localhost:8081"

SYNDICATE_OPENCLOUD_USER="jcnelson@cs.princeton.edu"
SYNDICATE_OPENCLOUD_PASSWORD=None
SYNDICATE_OPENCLOUD_PKEY="/home/jude/Desktop/research/git/syndicate/ms/tests/user_test_key.pem"

SYNDICATE_PYTHONPATH="/home/jude/Desktop/research/git/syndicate/build/out/python"

SYNDICATE_OBSERVER_PRIVATE_KEY="/home/jude/Desktop/research/git/syndicate/opencloud/syndicate_observer/syndicatelib_config/pollserver.pem"
SYNDICATE_OBSERVER_SECRET="/home/jude/Desktop/research/git/syndicate/opencloud/syndicate_observer/syndicatelib_config/observer_secret.txt"

SYNDICATE_OBSERVER_HTTP_PORT=65321

SYNDICATE_RG_CLOSURE="/home/jude/Desktop/research/git/syndicate/build/out/python/syndicate/rg/drivers/disk"
SYNDICATE_RG_DEFAULT_PORT=38800

SYNDICATE_UG_QUOTA=10
SYNDICATE_RG_QUOTA=10

SYNDICATE_OBSERVER_STORAGE_BACKEND="disk"

SYNDICATE_GATEWAY_NAME_PREFIX="OpenCloud"

DEBUG=True
