#!/usr/bin/python

import os
import sys

PATH_PATHS="AG-genbank.out.paths.txt"
PATH_PERMS="AG-genbank.out.perm.txt"
PATH_FILE_IDS="AG-genbank.out.file_id.txt"
PATH_FILE_VERSIONS="AG-genbank.out.file_version.txt"

fd_paths = open( PATH_PATHS, "r" )
fd_perms = open( PATH_PERMS, "r" )
fd_file_ids = open( PATH_FILE_IDS, "r" )
fd_file_versions = open( PATH_FILE_VERSIONS, "r")

while True:

    next_path = fd_paths.readline()
    next_perm = fd_perms.readline()
    next_file_id = fd_file_ids.readline()
    next_file_version = fd_file_versions.readline()

    if len(next_path) == 0 or len(next_perm) == 0 or len(next_file_id) == 0 or len(next_file_version) == 0:
        break;

    next_path = next_path.strip()
    next_perm = next_perm.strip()
    next_file_id = next_file_id.strip()
    next_file_version = next_file_version.strip()

    file_type = None
    if next_perm == "444":
        file_type = "f"
    else:
        file_type = "d"

    print "%s:/SYNDICATE-DATA/0%s.%s.%s/0.0" % (file_type, next_path, next_file_id, next_file_version)


fd_paths.close()
fd_perms.close()
fd_file_ids.close()
fd_file_versions.close()

