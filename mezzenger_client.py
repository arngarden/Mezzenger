#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2012 Gustav Arngården
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import threading
import zmq
import message

SERVER = '127.0.0.1'
SUB_PORT = 7201
SEND_PORT = 7202

DEFAULT_SEND_TIMEOUT = 2
DEFAULT_CONNECTION_RETRIES = 5

class MezzengerException(Exception):
    pass

class Mezzenger(threading.Thread):
    def __init__(self, server_address=SERVER, send_port=SEND_PORT, sub_port=SUB_PORT, send_timeout=DEFAULT_SEND_TIMEOUT,
                 connection_retries=DEFAULT_CONNECTION_RETRIES, auto_start=True, verbose=False):
        """ Init Mezzenger
        Args:
        - server_address: Server for receiving messages
        - send_port: Port for outgoing messages
        - sub_port: Port for incoming messages
        - send_timeout: Client tries sending message for connection_retries * send_timeout seconds
        - connection_retries: Client tries sending message for connection_retries * send_timeout seconds
        - auto_start: If True, thread is started at end of init
        - verbose: If True, print status information

        """
        threading.Thread.__init__(self)
        self.server_address = server_address
        self.send_port = send_port
        self.sub_port = sub_port
        self.send_timeout = send_timeout * 1000
        self.connection_retries = connection_retries
        self.verbose = verbose
        self.context = zmq.Context()
        self.sub_poller = zmq.Poller()
        self.msg_poller = zmq.Poller()
        self.msg_sub_socket = None
        self.msg_send_socket = None
        self.connect()
        self._stop = threading.Event()
        self.send_lock = threading.RLock()
        self.msg_map = {}
        self.channels = set()
        if auto_start:
            self.start()

    def connect(self):
        """ Connect to sockets.
        Send ping to server to make sure connection is established.

        """
        self._reconnect_sub_socket()
        self._reconnect_send_socket()
        ret = self._send('ping', 'ping')
        if not ret == 'OK':
            raise MezzengerException('Could not connect to server')

    def _reconnect_send_socket(self):
        """ Connect to send-socket.

        """
        if self.verbose:
            print 'Connecting send socket: %s:%s' % (self.server_address, self.send_port)
        if self.msg_send_socket:
            self.sub_poller.unregister(self.msg_send_socket)
            self.msg_send_socket.setsockopt(zmq.LINGER, 0)
            self.msg_send_socket.close()
        self.msg_send_socket = self.context.socket(zmq.REQ)
        self.msg_send_socket.connect('tcp://%s:%s' % (self.server_address, self.send_port))

        self.sub_poller.register(self.msg_send_socket, zmq.POLLIN)

    def _reconnect_sub_socket(self):
        """ Connect to sub-socket.

        """
        if self.verbose:
            print 'Connecting sub socket: %s:%s' % (self.server_address, self.sub_port)
        if self.msg_sub_socket:
            self.msg_poller.unregister(self.msg_sub_socket)
            self.msg_sub_socket.close()
        self.msg_sub_socket = self.context.socket(zmq.SUB)
        self.msg_sub_socket.connect('tcp://%s:%s' % (self.server_address, self.sub_port))
        self.msg_poller.register(self.msg_sub_socket, zmq.POLLIN)

    def _close_sockets(self):
        """ Close sockets.
        
        """
        self.msg_send_socket.setsockopt(zmq.LINGER, 0)
        self.msg_send_socket.close()
        self.msg_sub_socket.close()
        
    def subscribe(self, msg_name, callback):
        """ Start subscribing to msg_name.

        """
        if self.stopped():
            raise MezzengerException('Cannot subscribe to message, client is not running')
        if not msg_name in self.msg_map:
            self.msg_sub_socket.setsockopt(zmq.SUBSCRIBE, msg_name)
        self.msg_map[msg_name] = callback

    def unsubscribe(self, msg_name):
        """ Remove subscription of msg_name, raise exception if we do not subscribe to msg_name.
        
        """
        if msg_name in self.msg_map:
            self.msg_sub_socket.setsockopt(zmq.UNSUBSCRIBE, msg_name)
            _ = self.msg_map.pop(msg_name)
        else:
            raise MezzengerException('You do not subscribe to msg: %s' % msg_name)

    def _send(self, msg_name, payload, ack=0):
        """ Send message to server, returning return-message from server.
        If message can not be sent, return False
        
        """
        msg = message.Message(msg_name, payload=payload, ack=ack)
        if self.verbose:
            print 'Sending message: %s' % str(msg)
        self.msg_send_socket.send(msg.serialize())
        socks = dict(self.sub_poller.poll(self.send_timeout))
        if socks.get(self.msg_send_socket) == zmq.POLLIN:
            return self.msg_send_socket.recv()
        else:
            if self.verbose:
                print 'Timed out waiting for reply from server'
            for i in xrange(self.connection_retries):
                self._reconnect_send_socket()
                socks = dict(self.sub_poller.poll(self.send_timeout))
                if socks.get(self.msg_send_socket) == zmq.POLLIN:
                    return self.msg_send_socket.recv()
            self._reconnect_send_socket()
            if self.verbose:
                print 'Could not send message %s, server unreachable' % msg_name
            return False

    # TODO: override timeout
    def send_message(self, msg_name, payload, ack=0):
        """ Send message to server.
        Args:
        - msg_name: Name of message (= name of subscription channel)
        - payload: Message payload
        - ack: If > 0, server will make sure at least one client receives message

        """
        self.send_lock.acquire()
        ret = self._send(msg_name, payload, ack)
        self.send_lock.release()
        if not ret:
            raise MezzengerException('Could not send message to server')
        
    def run(self):
        """ Main loop.
        Listens for incoming messages.
        
        """
        while not self.stopped():
            socks = dict(self.msg_poller.poll(2000))
            if socks.get(self.msg_sub_socket) == zmq.POLLIN:
                msg = self.msg_sub_socket.recv()
                try:
                    msg = message.parse(msg)
                except Exception, e:
                    raise MezzengerException('Could not parse message: %s, %s' % (str(msg), e))
                if msg['name'] in self.msg_map:
                    if self.verbose:
                        print 'Got message: %s' % str(msg)
                    self.msg_map[msg['name']](msg['payload'], msg)
                    if msg['ack'] > 0:
                        # send back ack to server
                        if self.verbose:
                            print 'Ack-ing message: %s' % msg['checksum']
                        self.send_message('ack', msg['checksum'])
        self._close_sockets()
        print 'Stopped'
        
    def stop(self):
        """ Stop client.
        """
        self._stop.set()

    def stopped(self):
        """ Check if client is stopped.
        """
        return self._stop.isSet()

    
    
    
