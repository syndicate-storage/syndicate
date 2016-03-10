# Syndicate Gateway Docker Image for Node.js

Build a Docker Image
--------------------

Make sure that you install docker and have access rights to run docker before get started.

Run build.sh to start building a docker image.
```
sudo build.sh
```

You can also specify a port number you want to use for the gateway and a name of docker image.
```
sudo build.sh 31112 syndicate-gateways-nodejs-31112
```


A new docker image for Syndicate Gateway will have a name "syndicate-gateways-nodejs" by default. The docker image created will have Syndicate installed and an user "syndicate".

You can run the image in interactive mode by following command.
```
sudo docker run -t -i -p 31111:31111 syndicate-gateways-nodejs
```

