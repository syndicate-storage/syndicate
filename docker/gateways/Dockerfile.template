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
RUN apt-get install -y python-pip

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
RUN pip install pika python-irodsclient retrying timeout_decorator

USER syndicate
WORKDIR $HOME

RUN wget -O syndicate.zip https://github.com/jcnelson/syndicate/archive/master.zip
RUN unzip syndicate.zip
RUN mv syndicate-master syndicate
WORKDIR "syndicate"
RUN make

USER root
RUN make -C libsyndicate install
RUN make -C libsyndicate-ug install
RUN make -C python install

WORKDIR $HOME/syndicate/gateways/acquisition
RUN make install

WORKDIR $HOME/syndicate/gateways/user
RUN make install

expose $GATEWAY_PORT$

USER syndicate
WORKDIR $HOME
