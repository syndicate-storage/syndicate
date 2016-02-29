# Syndicate Gateway Docker Image

Build a Docker Image
------------------

Make sure that you install docker and have access rights to run docker before get started.

Run build.sh to start building a docker image.
```
sudo build.sh
```

A new docker image for Syndicate Gateway will have a name "syndicate-gateways" by default. The docker image created will have Syndicate installed and an user "syndicate".

You can run the image in interactive mode by following command.
```
sudo docker run -t -i syndicate-gateways
```


