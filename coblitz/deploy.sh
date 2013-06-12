#!/bin/sh

for node in $(cat nodelist.txt); do 
   echo $node
   scp -o ConnectTimeout=5 -o StrictHostKeyChecking=no tester.py princeton_syndicate@$node:~/ && 
   ssh -o StrictHostKeyChecking=no princeton_syndicate@$node "killall -9 tester.py; killall -9 python; killall -9 syndicatefs; syndicatefs-umount /home/princeton_syndicate/syndicate/client/tmp; rm -rf /tmp/tmp*; nohup python ~/tester.py </dev/null >/dev/null 2>/dev/null &"; 
   echo ""
done
