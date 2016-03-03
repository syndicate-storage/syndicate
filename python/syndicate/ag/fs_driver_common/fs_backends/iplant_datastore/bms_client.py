#!/usr/bin/env python

"""
   Copyright 2014 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import logging
import pika
import json
import string
import random
import threading

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

BMS_REGISTRATION_EXCHANGE = 'bms_registrations'
BMS_REGISTRATION_QUEUE = 'bms_registrations'

BMS_REREGISTRATION_SEC = 5*60

"""
Interface class to iPlant Border Message Server
"""
class bms_registration_result_client(object):
    def __init__(self, user_id=None,
                       application_name=None):
        self.user_id = user_id
        self.application_name = application_name

    @classmethod
    def fromDict(cls, dictionary):
        return bms_registration_result_client(dictionary['user_id'], dictionary['application_name'])

    def __repr__(self): 
        return "<bms_registration_result_client %s %s>" % (self.user_id, self.application_name) 

class bms_registration_result(object):
    def __init__(self, client=None, 
                       lease_start=0,
                       lease_expire=0):
        self.client = client
        self.lease_start = lease_start
        self.lease_expire = lease_expire

    @classmethod
    def fromJson(cls, json_string):
        if bms_registration_result.isRegistrationJson(json_string):
            msg = json.loads(json_string)
            return bms_registration_result(client=bms_registration_result_client.fromDict(msg['client']), 
                                lease_start=msg['lease_start'],
                                lease_expire=msg['lease_expire'])
        else:
            return None

    @classmethod
    def isRegistrationJson(cls, json_string):
        if json_string and len(json_string) > 0:
            msg = json.loads(json_string)
            if ('client' in msg) and ('lease_start' in msg) and ('lease_expire' in msg):
                return True
        return False

    def __repr__(self): 
        return "<bms_registration_result %s %d %d>" % (self.client, self.lease_start, self.lease_expire) 

class bms_message_acceptor(object):
    def __init__(self, acceptor="path", 
                       pattern="*"):
        self.acceptor = acceptor
        self.pattern = pattern

    def asDict(self):
        return self.__dict__

    def __repr__(self): 
        return "<bms_message_acceptor %s %s>" % (self.acceptor, self.pattern) 

class bms_client(object):
    def __init__(self, host=None,
                       port=31333,
                       vhost="/",
                       user=None,
                       password=None,
                       appid=None,
                       auto_reregistration=True,
                       acceptors=None
                       ):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.user = user
        self.password = password
        if appid:
            self.appid = appid
        else:
            self.appid = self._generateAppid()

        self.connection = None
        self.channel = None
        self.queue = None
        self.closing = False
        self.consumer_tag = None
        self.consumer_thread = None
        self.registration_msg = None
        self.registration_timer = None
        self.auto_reregistration = auto_reregistration
        self.acceptors = acceptors

        self.on_connect_callback = None
        self.on_register_callback = None
        self.on_message_callback = None

    def setCallbacks(self, on_connect_callback=None, on_register_callback=None, on_message_callback=None):
        if on_connect_callback:
            self.on_connect_callback = on_connect_callback
        if on_register_callback:
            self.on_register_callback = on_register_callback
        if on_message_callback:
            self.on_message_callback = on_message_callback

    def clearCallbacks(self):
        self.on_connect_callback = None
        self.on_register_callback = None
        self.on_message_callback = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _generateId(self, size=8, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def _generateAppid(self):
        return self._generateId()

    def _consumerThreadTask(self):
        log.info('start consumer thread task')
        self.connection.ioloop.start()
        log.info('stop consumer thread task')

    def connect(self):
        log.info('connect to %s', self.host)
        credentials = pika.PlainCredentials(self.user, 
                                            self.password)
        parameters = pika.ConnectionParameters(self.host, 
                                               self.port, 
                                               self.vhost, 
                                               credentials)
        self.connection = pika.SelectConnection(parameters,
                                                self._onConnectionOpen,
                                                stop_ioloop_on_close=False)
        log.info('start consuming messages')
        self.consumer_thread = threading.Thread(target=self._consumerThreadTask)
        self.consumer_thread.start()

    def _onConnectionOpen(self, connection):
        log.info('connected')
        self.connection.add_on_close_callback(self._onConnectionClosed)
        # open a channel
        self.connection.channel(on_open_callback=self._onChannelOpen)

    def _onConnectionClosed(self, connection, reply_code, reply_text):
        log.info('connection closed')
        self.channel = None
        if self.closing:
            self.connection.ioloop.stop()
        else:
            log.info('reconnect')
            self.connection.add_timeout(5, self.reconnect)

    def _onChannelOpen(self, channel):
        log.info('open a channel')
        self.channel = channel
        self.channel.add_on_close_callback(self._onChannelClosed)

        log.info('declare a queue %s/%s', self.user, self.appid)
        # declare a queue
        self.queue = self.user + "/" + self.appid
        self.channel.queue_declare(self._onQueueDeclareok, 
                                   queue=self.queue,
                                   durable=False, 
                                   exclusive=False, 
                                   auto_delete=True)

    def _onQueueDeclareok(self, mothod_frame):
        log.info('declared a queue %s/%s', self.user, self.appid)
        # set consumer
        self.channel.add_on_cancel_callback(self._onConsumerCancelled)
        self.consumer_tag = self.channel.basic_consume(self._onMessage, 
                                                       queue=self.queue,
                                                       no_ack=False)

        # call callback
        if self.on_connect_callback:
            self.on_connect_callback()

        # register automatically
        if self.auto_reregistration:
            if self.acceptors:
                self.register(self.acceptors)

    def _onChannelClosed(self, channel, reply_code, reply_text):
        log.info('channel closed')
        if self.registration_timer:
            self.registration_timer.cancel()
            self.registration_timer = None
        self.connection.close()

    def _onConsumerCancelled(self, method_frame):
        log.info('consumer cancelled')
        if self.channel:
            self.channel.close()

    def _onMessage(self, channel, method, properties, body):
        log.info('Received message # %s from %s: %s',
                    method.delivery_tag, properties.app_id, body)

        # acknowledge
        self.channel.basic_ack(method.delivery_tag)

        # call callback
        # check if a message is registration message
        if bms_registration_result.isRegistrationJson(body):
            if self.on_register_callback:
                self.on_register_callback(bms_registration_result.fromJson(body))
        else:
            if self.on_message_callback:
                self.on_message_callback(body)

    def reconnect(self):
        self.connection.ioloop.stop()

        if not self.closing:
            self.connection = self.connect()

    def close(self):
        log.info('stopping')
        self.closing = True

        if self.channel:
            self.channel.basic_cancel(self._onCancelok, self.consumer_tag)

        self.connection.ioloop.start()
        self.connection.close()

        self.consumer_thread = None

    def _onCancelok(self, unused_frame):
        self.channel.close()

    def reRegister(self):
        if self.channel:
            log.info('re-register')
            if self.registration_msg:
                self._registerByString(self.registration_msg)

    def _registerByString(self, msg):
        self.registration_msg = msg
        # set a message property
        prop = pika.BasicProperties(reply_to=self.queue)

        # request a registration
        self.channel.basic_publish(exchange=BMS_REGISTRATION_EXCHANGE,
                                   routing_key=BMS_REGISTRATION_QUEUE,
                                   properties=prop,
                                   body=msg)
        
        if self.registration_timer:
            self.registration_timer.cancel()

        if self.auto_reregistration:
            self.registration_timer = threading.Timer(BMS_REREGISTRATION_SEC, self.reRegister)
            self.registration_timer.start()

    def register(self, acceptors):
        log.info('register')
        # make a registration message
        """
        reg_msg = {"request": "lease", 
                    "client": {"user_id": self.user,
                                "application_name": self.appid},
                    "acceptors": [{"acceptor": "path",
                                    "pattern": "/iplant/home/iychoi/*"}] }
        """
        acceptor_arr = []
        for acceptor in acceptors:
            acceptor_arr.append(acceptor.asDict())

        reg_msg = {"request": "lease", 
                    "client": {"user_id": self.user,
                                "application_name": self.appid},
                    "acceptors": acceptor_arr}
        reg_msg_str = json.dumps(reg_msg)

        self._registerByString(reg_msg_str)

