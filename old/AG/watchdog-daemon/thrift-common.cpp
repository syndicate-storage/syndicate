/*
   Copyright 2013 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#include <thrift-common.h>

thrift_connection* thrift_connect(string addr, int port, bool is_wd) {
    thrift_connection *tc = new thrift_connection;
    tc->socket = boost::make_shared<TSocket>(addr, port);
    tc->transport = boost::make_shared<TFramedTransport>(tc->socket);
    tc->protocol = boost::make_shared<TBinaryProtocol>(tc->transport);
    tc->err = NULL;
    tc->is_connected = true;
    if (is_wd) {
	tc->wd_client = new WDDaemonClient(tc->protocol);
	tc->ag_client = NULL;
    }
    else {
	tc->ag_client = new AGDaemonClient(tc->protocol);
	tc->wd_client = NULL;
    }
    try {
	tc->transport->open();
    }
    catch (TException &tx) {
	tc->err = strdup(tx.what());
	tc->is_connected = false;
    }
    return tc;
}

void thrift_disconnect(thrift_connection *tc) {
    tc->transport->close();
    tc->socket.reset();
    tc->transport.reset();
    tc->protocol.reset();
    if (tc->wd_client)
	delete tc->wd_client;
    if (tc->ag_client)
	delete tc->ag_client;
    if (tc->err)
	free(tc->err);
    delete tc;
}

