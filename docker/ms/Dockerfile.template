# Syndicate gateways
#
# VERSION	1.0

FROM	ubuntu:14.04
MAINTAINER	Illyoung Choi <iychoi@email.arizona.edu>

##############################################
# Setup utility packages
##############################################

RUN apt-get update
RUN apt-get install -y wget unzip

##############################################
# Setup a Syndicate account
##############################################
ENV HOME /home/syndicate

RUN useradd syndicate && echo 'syndicate:docker' | chpasswd
RUN echo "syndicate ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN mkdir /home/syndicate
RUN chown -R syndicate:syndicate $HOME

##############################################
# build essentials
##############################################
RUN apt-get install -y build-essential

##############################################
# fskit
##############################################
RUN apt-get install -y libfuse-dev libattr1-dev

USER syndicate
WORKDIR $HOME

RUN wget -O fskit.zip https://github.com/jcnelson/fskit/archive/master.zip
RUN unzip fskit.zip
RUN mv fskit-master fskit
WORKDIR "fskit"
RUN make

USER root
RUN make install

##############################################
# syndicate
##############################################
RUN apt-get install -y protobuf-compiler libprotobuf-dev libcurl4-gnutls-dev libmicrohttpd-dev libjson0-dev valgrind cython python-protobuf libssl-dev python-crypto python-requests

USER syndicate
WORKDIR $HOME

RUN wget -O syndicate.zip https://github.com/jcnelson/syndicate/archive/master.zip
RUN unzip syndicate.zip
RUN mv syndicate-master syndicate
WORKDIR "syndicate"

# replace localhost to $MS_HOST$
RUN sed -i 's/localhost/$MS_HOST$/g' ms/common/msconfig.py

RUN make MS_APP_ADMIN_EMAIL=$MS_APP_ADMIN_EMAIL$
RUN echo "======== SYNDICATE ADMIN INFO ========"
RUN cat build/out/ms/common/admin_info.py

USER root
RUN make -C libsyndicate install
RUN make -C libsyndicate-ug install
RUN make -C python install

WORKDIR $HOME/syndicate/gateways/acquisition
RUN make install

WORKDIR $HOME/syndicate/gateways/user
RUN make install

expose 31111

##############################################
# google app engine
##############################################
USER syndicate
WORKDIR $HOME

RUN wget https://storage.googleapis.com/appengine-sdks/featured/google_appengine_1.9.33.zip
RUN unzip google_appengine_1.9.33.zip

expose 8080
expose 8000

##############################################
# Run Syndicate MS
##############################################
USER syndicate
WORKDIR $HOME
RUN mkdir /home/syndicate/datastore

CMD ["/home/syndicate/google_appengine/dev_appserver.py", "--admin_host=0.0.0.0", "--host=0.0.0.0", "--storage_path=/home/syndicate/datastore", "/home/syndicate/syndicate/build/out/ms"]

