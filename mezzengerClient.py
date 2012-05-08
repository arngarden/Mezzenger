#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2012 Gustav Arngården
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import sys
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
    def __init__(self, serverAddress=SERVER, sendPort=SEND_PORT, subPort=SUB_PORT, sendTimeout=DEFAULT_SEND_TIMEOUT,
                 connectionRetries=DEFAULT_CONNECTION_RETRIES, autoStart=True, verbose=False):
        """ Init Mezzenger
        Args:
        - serverAddress: Server for receiving messages
        - sendPort: Port for outgoing messages
        - subPort: Port for incoming messages
        - sendTimeout: Client tries sending message for connectionRetries * sendTimeout seconds
        - connectionRetries: Client tries sending message for connectionRetries * sendTimeout seconds
        - autoStart: If True, thread is started at end of init
        - verbose: If True, print status information

        """
        threading.Thread.__init__(self)
        self.serverAddress = serverAddress
        self.sendPort = sendPort
        self.subPort = subPort
        self.sendTimeout = sendTimeout * 1000
        self.connectionRetries = connectionRetries
        self.verbose = verbose
        self.context = zmq.Context()
        self.subPoller = zmq.Poller()
        self.msgPoller = zmq.Poller()
        self.msgSubSocket = None
        self.msgSendSocket = None
        self.connect()
        self._stop = threading.Event()
        self.msgMap = {}
        self.channels = set()
        if autoStart:
            self.start()

    def connect(self):
        """ Connect to sockets.
        Send ping to server to make sure connection is established.

        """
        self._reconnectSubSocket()
        self._reconnectSendSocket()
        ret = self._send('ping', 'ping')
        if not ret == 'OK':
            raise MezzengerException('Could not connect to server')

    def _reconnectSendSocket(self):
        """ Connect to send-socket.

        """
        if self.verbose:
            print 'Connecting send socket: %s:%s' % (self.serverAddress, self.sendPort)
        if self.msgSendSocket:
            self.subPoller.unregister(self.msgSendSocket)
            self.msgSendSocket.setsockopt(zmq.LINGER, 0)
            self.msgSendSocket.close()
        self.msgSendSocket = self.context.socket(zmq.REQ)
        self.msgSendSocket.connect('tcp://%s:%s' % (self.serverAddress, self.sendPort))

        self.subPoller.register(self.msgSendSocket, zmq.POLLIN)

    def _reconnectSubSocket(self):
        """ Connect to sub-socket.

        """
        if self.verbose:
            print 'Connecting sub socket: %s:%s' % (self.serverAddress, self.subPort)
        if self.msgSubSocket:
            self.msgPoller.unregister(self.msgSubSocket)
            self.msgSubSocket.close()
        self.msgSubSocket = self.context.socket(zmq.SUB)
        self.msgSubSocket.connect('tcp://%s:%s' % (self.serverAddress, self.subPort))
        self.msgPoller.register(self.msgSubSocket, zmq.POLLIN)

    def _closeSockets(self):
        """ Close sockets.
        
        """
        self.msgSendSocket.setsockopt(zmq.LINGER, 0)
        self.msgSendSocket.close()
        self.msgSubSocket.close()
        
    def subscribe(self, msgName, callback):
        """ Start subscribing to msgName.

        """
        if self.stopped():
            raise MezzengerException('Cannot subscribe to message, client is not running')
        if not msgName in self.msgMap:
            self.msgSubSocket.setsockopt(zmq.SUBSCRIBE, msgName)
        self.msgMap[msgName] = callback

    def unsubscribe(self, msgName):
        """ Remove subscription of msgName, raise exception if we do not subscribe to msgName.
        
        """
        if msgName in self.msgMap:
            self.msgSubSocket.setsockopt(zmq.UNSUBSCRIBE, msgName)
            _ = self.msgMap.pop(msgName)
        else:
            raise MezzengerException('You do not subscribe to msg: %s' % msgName)

    def _send(self, msgName, payload, ack=0):
        """ Send message to server, returning return-message from server.
        If message can not be sent, return False
        
        """
        msg = message.Message(msgName, payload=payload, ack=ack)
        if self.verbose:
            print 'Sending message: %s' % str(msg)
        self.msgSendSocket.send(msg.serialize())
        socks = dict(self.subPoller.poll(self.sendTimeout))
        if socks.get(self.msgSendSocket) == zmq.POLLIN:
            return self.msgSendSocket.recv()
        else:
            if self.verbose:
                print 'Timed out waiting for reply from server'
            for i in xrange(self.connectionRetries):
                self._reconnectSendSocket()
                socks = dict(self.subPoller.poll(self.sendTimeout))
                if socks.get(self.msgSendSocket) == zmq.POLLIN:
                    return self.msgSendSocket.recv()
            self._reconnectSendSocket()
            if self.verbose:
                print 'Could not send message %s, server unreachable' % msgName
            return False

    # TODO: override timeout
    def sendMessage(self, msgName, payload, ack=0):
        """ Send message to server.
        Args:
        - msgName: Name of message (= name of subscription channel)
        - payload: Message payload
        - ack: If > 0, server will make sure at least one client receives message

        """
        ret = self._send(msgName, payload, ack)
        if not ret:
            raise MezzengerException('Could not send message to server')
        
    def run(self):
        """ Main loop.
        Listens for incoming messages.
        
        """
        while not self.stopped():
            socks = dict(self.msgPoller.poll(2000))
            if socks.get(self.msgSubSocket) == zmq.POLLIN:
                msg = self.msgSubSocket.recv()
                try:
                    msg = message.parse(msg)
                except Exception, e:
                    raise MezzengerException('Could not parse message: %s, %s' % (str(msg), e))
                if msg['name'] in self.msgMap:
                    if self.verbose:
                        print 'Got message: %s' % str(msg)
                    self.msgMap[msg['name']](msg['payload'], msg)
                    if msg['ack'] > 0:
                        # send back ack to server
                        if self.verbose:
                            print 'Ack-ing message: %s' % msg['checksum']
                        self.sendMessage('ack', msg['checksum'])
        self._closeSockets()
        print 'Stopped'
        
    def stop(self):
        """ Stop client.
        """
        self._stop.set()

    def stopped(self):
        """ Check if client is stopped.
        """
        return self._stop.isSet()

    
    
    
