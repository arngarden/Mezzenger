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
#from message import Message

RECV_SERVER = '127.0.0.1:7201'
SEND_SERVER = '127.0.0.1:7202'

DEFAULT_SEND_TIMEOUT = 2000
DEFAULT_CONNECTION_RETRIES = 5

class MezzengerException(Exception):
    pass

class Mezzenger(threading.Thread):
    def __init__(self, recvServer=None, sendServer=None, sendTimeout=None,
                 connectionRetries=None, autoStart=True, verbose=True):
        """ Init Mezzenger
        Args:
        - recvServer: Server for receiving messages
        - sendServer: Server for sending messages
        - sendTimeout: Client tries sending message for connectionRetries * sendTimeout seconds
        - connectionRetries: Client tries sending message for connectionRetries * sendTimeout seconds
        - autoStart: If True, thread is started at end of init
        - verbose: If True, print status information

        """
        threading.Thread.__init__(self)
        self.recvServer = recvServer or RECV_SERVER
        self.sendServer = sendServer or SEND_SERVER
        self.sendTimeout = sendTimeout or DEFAULT_SEND_TIMEOUT
        self.connectionRetries = connectionRetries or DEFAULT_CONNECTION_RETRIES
        self.verbose = verbose
        self.context = zmq.Context()
        self.recvPoller = zmq.Poller()
        self.msgPoller = zmq.Poller()
        self.msgRecvSocket = None
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
        self._reconnectRecvSocket()
        self._reconnectSendSocket()
        ret = self._send('ping', 'ping')
        if not ret == 'OK':
            raise MezzengerException('Could not connect to server')

    def _reconnectSendSocket(self):
        """ Connect to send-socket.

        """
        if self.verbose:
            print 'Connecting send socket: %s' % self.sendServer
        if self.msgSendSocket:
            self.recvPoller.unregister(self.msgSendSocket)
            self.msgSendSocket.setsockopt(zmq.LINGER, 0) #????
            self.msgSendSocket.close()
        self.msgSendSocket = self.context.socket(zmq.REQ)
        self.msgSendSocket.connect('tcp://%s' % self.sendServer)

        self.recvPoller.register(self.msgSendSocket, zmq.POLLIN)

    def _reconnectRecvSocket(self):
        """ Connect to recv-socket.

        """
        if self.verbose:
            print 'Connecting recv socket: %s' % self.recvServer
        if self.msgRecvSocket:
            self.msgPoller.unregister(self.msgRecvSocket)
            self.msgRecvSocket.close()
        self.msgRecvSocket = self.context.socket(zmq.SUB)
        self.msgRecvSocket.connect('tcp://%s' % self.recvServer)
        self.msgPoller.register(self.msgRecvSocket, zmq.POLLIN)

    def _closeSockets(self):
        """ Close sockets.
        
        """
        self.msgSendSocket.setsockopt(zmq.LINGER, 0)
        self.msgSendSocket.close()
        self.msgRecvSocket.close()
        
    def subscribe(self, msgName, callback):
        """ Start subscribing to msgName.

        """
        if not msgName in self.msgMap:
            self.msgMap[msgName] = callback
            self.msgRecvSocket.setsockopt(zmq.SUBSCRIBE, msgName)

    def unsubscribe(self, msgName):
        """ Remove subscription of msgName, raise exception if we do not subscribe to msgName.
        
        """
        if msgName in self.msgMap:
            self.msgRecvSocket.setsockopt(zmq.UNSUBSCRIBE, msgName)
            _ = self.msgMap.pop(msgName)
        else:
            raise MezzengerException('You do not subscribe to msg: %s' % msgName)

    def _send(self, msgName, msg, ack=0):
        """ Send message to server, returning return-message from server.
        If message can not be sent, return False
        
        """
        msg = message.Message(msgName, msg=msg, ack=ack)
        if self.verbose:
            print 'Sending message: %s' % str(msg)
        self.msgSendSocket.send(msg.serialize())
        socks = dict(self.recvPoller.poll(self.sendTimeout))
        if socks.get(self.msgSendSocket) == zmq.POLLIN:
            return self.msgSendSocket.recv()
        else:
            if self.verbose:
                print 'Timed out waiting for reply from server'
            for i in xrange(self.connectionRetries):
                self._reconnectSendSocket()
                socks = dict(self.recvPoller.poll(self.sendTimeout))
                if socks.get(self.msgSendSocket) == zmq.POLLIN:
                    return self.msgSendSocket.recv()
            self._reconnectSendSocket()
            if self.verbose:
                print 'Could not send message %s, server unreachable' % msgName
            return False

    # TODO: override timeout
    def sendMessage(self, msgName, msg, ack=0):
        """ Send message to server.
        Args:
        - msgName: Name of message (= name of subscription channel)
        - msg: Message body
        - ack: If > 0, server will make sure at least one client receives message

        """
        ret = self._send(msgName, msg, ack)
        if not ret:
            raise MezzengerException('Could not send message to server')
        
    def run(self):
        """ Main loop.
        Listens for incoming messages.
        
        """
        while not self.stopped():
            socks = dict(self.msgPoller.poll(2000))
            if socks.get(self.msgRecvSocket) == zmq.POLLIN:
                msg = self.msgRecvSocket.recv()
                try:
                    msg = message.parse(msg)
                except Exception, e:
                    raise MezzengerException('Could not parse message: %s, %s' % (str(msg), e))
                if msg['name'] in self.msgMap:
                    if self.verbose:
                        print 'Got message: %s' % str(msg)
                    self.msgMap[msg['name']](msg['msg'])
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

    
    
    
