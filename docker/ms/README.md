# Syndicate MS Docker Image

Build a Docker Image
--------------------

Make sure that you install docker and have access rights to run docker before get started.

Run build.sh to start building a docker image.
```
sudo build.sh "admin@email.addr"
```

A new docker image for Syndicate MS will have a name "syndicate-ms" by default. The docker image created will have Syndicate installed and an user "syndicate".

You can run the image in interactive mode by following command.
```
sudo docker run -t -i -p 8080:8080 -p 8000:8000 syndicate-ms
```

Also, you can open a terminal to the running image (container).
```
sudo docker exec -i -t <container_id> bash
```
