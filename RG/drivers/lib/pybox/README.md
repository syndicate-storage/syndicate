pybox
=====

INTRODUCTION
------------

`pybox` is a Python API/client that manipulates files on box.com
(a.k.a box.net). It can display user's account information, file structure,
and file information, manage(move/rename/delete/upload/download) files and
directories, and **most importantly**, it can compare and synchronize
directories between client and server.

USAGE
-----

Please take the following steps:

1. Obtain an API key from [here](http://www.box.net/developers/services).

2. Copy boxrc.example to user's home directory, rename it to .boxrc in a POSIX system
(e.g. Unix, Linux, Mac OS X) or \_boxrc in a non-POSIX system(e.g. Windows).

3. Edit .boxrc/\_boxrc: replace api\_key's value(YOUR\_API\_KEY) with the API key
   you've got in step 1.

4. Copy box-logging.conf.example to box-logging.conf.

5. Edit(optionally) box-logging.conf, e.g. change 'BOX.LOG' to different name
   or path. If you'd like to put this log configuration file to a different 
   directory, don't forget to add an environment variable named LOG\_CONF\_DIR.
   
6. Open a command terminal, change directory to the pybox directory, then run:

    python pybox/boxclient.py -U YOUR\_LOGIN\_EMAIL -p -a

    Replace the above email with your actual login email on box.net,
    and type your password when prompted. If login/password combination is
    correct, you will get your auth token from the output(something like
    "auth\_token: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

7. Edit .boxrc/\_boxrc again. Replace(or add) account section(YOUR\_ACCOUNT\_LOGIN)
   with your login(actually, it can be any string, not necessarily the same as
   your box login email), replace auth\_token value(YOUR\_AUTH\_TOKEN) with the one
   you've got in step 6.

8. If you have multiple box accounts, just repeat step 6 and 7.

If everything goes smoothly, you are now free to manipulate your files on the
box account(s) without typing password any more. Generally the command is:

    python pybox/boxclient.py -U YOUR_LOGIN [options] [args]

Please be noticed that this time YOUR\_LOGIN is the string you set in step 7.

All supported options are listed as follows:

* _-U, --username_ specify username/email

* _-p, --password_ prompt password (only needed when auth\_token is not found)

* _-a, --auth-token_ print auth token

* _-I, --account-info_ get box account information

* _-t, --target_ specify target type(f for file&lt;default>, d for directory)

* _-l, --list_ list directory

* _-w, --what-id_ get a path(server-side)'s id

* _-i, --info_ get file information

* _-M, --mkdir_ make a directory

* _-R, --remove_ remove a file or directory

* _-m, --move_ move a file or directory

* _-r, --rename_ rename a file or directory

* _-1, --onelevel_ list one level files

* _-z, --zip_ list file tree in zip format

* _-N, --nofiles_ only list directory

* _-s, --simple_ show simple information

* _-c, --chdir_ change directory

* _-d, --download_ download file

* _-u, --upload_ upload file

* _-P, --plain-name_ use plain name(server-side) instead of id

* _-C, --compare_ compare local and remote directories

* _-S, --sync_ sync local and remote directories

* _-n, --dry-run_ show what would have been transferred when sync

* _-f, --from-file_ read arguments from file(arguments separated by line break)

EXAMPLES
--------

Assume all the following operations are performed on Bob's account.

* show account information:

        python pybox/boxclient.py -Ubob -I

* list all files(caution: this could be VERY VERY slow):

        python pybox/boxclient.py -Ubob -l 0 (0 is the root id)

* list all first-level files with fewer details:

        python pybox/boxclient.py -Ubob -ls1 0

* create a directory `dir1` under root:

        python pybox/boxclient.py -Ubob -M dir1

* create a directory `dir2` under `dir1`:

        python pybox/boxclient.py -Ubob -P -c dir1 -M dir2

* get directory `dir1/dir2`(starting from root)'s id:

        python pybox/boxclient.py -Ubob -w dir1/dir2

* upload file `file1`, `file2` and directory `dir3` to root directory:

        python pybox/boxclient.py -Ubob -u file1 file2 dir3

* upload `file3` to a directory whose id is `1005691453`

        python pybox/boxclient.py -Ubob -c1005691453 -u file3

* upload `file4` to a directory whose path is 'path1/path2'(starting from root)

        python pybox/boxclient.py -Ubob -P -c path1/path2 -u file4

* remove a file whose id is `1005181453`

        python pybox/boxclient.py -Ubob -R 1005181453

* remove a directory whose path is `path1/path2`(starting from root)

        python pybox/boxclient.py -Ubob -PR -td path1/path2

* rename file `file1` to `file1.new`, file `file2` to `file2.new`

        python pybox/boxclient.py -Ubob -Pr file1 file1.new file2 file2.new

* rename directory `dir1` to `dir2`

        python pybox/boxclient.py -Ubob -Pr -td dir1 dir2

* move a file with id `1025611460` to a directory with id `225236230`

        python pybox/boxclient.py -Ubob -m 1025611460 225236230

* move directory `dir1` to directory `dir2`, directory `dir3/dir4` to directory
  `dir5/dir6/dir7`

        python pybox/boxclient.py -Ubob -td -Pm dir1 dir2 dir3/dir4 dir5/dir6/dir7

* download a directory `dir1/dir2`

        python pybox/boxclient.py -Ubob -td -Pd dir1/dir2

* compare a local directory `/Users/bob/dir1` with a remote directory `dir2/dir3`

        python pybox/boxclient.py -Ubob -td -PC /Users/bob/dir1 dir2/dir3

* sync a local directory `/Users/bob/dir1`(source) with a remote directory
  `dir2/dir3`(destination)

        python pybox/boxclient.py -Ubob -PS /Users/bob/dir1 dir2/dir3


REFERENCE
---------

[Box API documentation](http://developers.box.net/w/page/12923958/FrontPage)


LICENSE
-------

Copyright 2011-2012 Hui Zheng

Released under the [MIT License](http://www.opensource.org/licenses/mit-license.php).

