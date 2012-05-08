#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2012 Gustav Arngården
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import sys
import os
import socket
import time
import threading
import cPickle
import argparse
import signal
import zmq
import message

DEFAULT_SERVER = '127.0.0.1'
DEFAULT_PUB_PORT = 7201
DEFAULT_RECV_PORT = 7202
MSG_CHECK_INTERVAL = 10

class MezzengerServerException(Exception):
    pass

class MezzengerServer:
    def __init__(self, bindAddress=None, pubPort=None, recvPort=None,
                 persistFile=None, verbose=False):
        """ Init server.
        Args:
        - bindAddress: Address to bind to
        - pubPort: Port for sending messages
        - recvPort: Port for receiving messages
        - persistFile: File for saving messages that are waiting for ack
        - verbose: Print verbose info if True
        
        """
        self.verbose = verbose
        self.pubPort = pubPort or DEFAULT_PUB_PORT
        self.recvPort = recvPort or DEFAULT_RECV_PORT
        self.bindAddress = bindAddress or DEFAULT_SERVER
        self.messageRecvSocket = None
        self.messageSocketPub = None
        try:
            self.context = zmq.Context()
            self.poller = zmq.Poller()
            self.connect()
        except zmq.ZMQError:
            raise MezzengerServerException('Could not connect sockets')
        self.persistFile = persistFile or ''
        if os.path.isfile(self.persistFile):
            try:
                with open(self.persistFile, 'rb') as f:
                    self.msgCollection = cPickle.load(f)
                print 'Found old msgCollection with %s messages' % len(self.msgCollection)
            except IOError:
                raise MezzengerServerException('Could not open persist file: %s' % self.persistFile)
        else:
            self.msgCollection = {}
        self._stop = threading.Event()
        signal.signal(signal.SIGINT, self.signalHandler)
        workThread = threading.Thread(target=self.work)
        workThread.start()
        checkMsgThread = threading.Thread(target=self.checkMsgQueue)
        checkMsgThread.start()
        while True:
            if self.stopped():
                break
            time.sleep(2)

        
    def connect(self):
        """ Connect sockets.
        """
        print 'Binding sockets'
        if self.messageRecvSocket:
            self.poller.unregister(self.messageRecvSocket)
            self.messageRecvSocket.setsockopt(zmq.LINGER, 0)
            self.messageRecvSocket.close()
        self.messageRecvSocket = self.context.socket(zmq.ROUTER)
        self.messageRecvSocket.bind('tcp://%s:%s' % (self.bindAddress, self.recvPort))
        self.poller.register(self.messageRecvSocket, zmq.POLLIN)
        if self.verbose:
            print 'Bound recv-socket at tcp://%s:%s' % (self.bindAddress, self.recvPort)
        if self.messageSocketPub:
            self.messageSocketPub.close()
        self.messageSocketPub = self.context.socket(zmq.PUB)
        self.messageSocketPub.bind('tcp://%s:%s' % (self.bindAddress, self.pubPort))
        if self.verbose:
            print 'Bound pub-socket at tcp://%s:%s' % (self.bindAddress, self.pubPort)

    def _closeSockets(self):
        """ Close sockets.
        """
        self.messageRecvSocket.setsockopt(zmq.LINGER, 0)
        self.messageRecvSocket.close()
        self.messageSocketPub.close()
        
    def work(self):
        """ Start main loop.
        Listen for incoming messages and resend them to subscribers.
        
        """
        print 'Starting work'
        while not self.stopped():
            try:
                socks = dict(self.poller.poll(5))
                if socks.get(self.messageRecvSocket) == zmq.POLLIN:
                    # received message from client
                    address = self.messageRecvSocket.recv()
                    _ = self.messageRecvSocket.recv()
                    msg = self.messageRecvSocket.recv()            
                    try:
                        msg = message.parse(msg)
                        if self.verbose:
                            print 'Got message: %s' % str(msg)
                    except:
                        print 'ERROR: Could not parse message: %s' % str(msg)
                        continue
                    if msg['name'] == 'ack':
                        # message is ack for previous sent message, checksum for ack:ed message is in payload
                        try:
                            if self.verbose:
                                print 'Got ack for message: %s' % msg['payload']
                            self.msgCollection.pop(msg['payload'])
                            if self.persistFile:
                                self._flushSavedMessages()
                        except KeyError:
                            pass
                    if int(msg['ack']) > 0:
                        # save message and make sure server receive ack
                        if self.verbose:
                            print 'Delivery of message with ack: %s' % msg['checksum']
                        self.msgCollection[msg['checksum']] = (time.time(), msg)
                        if self.persistFile:
                            self._flushSavedMessages()
                    self.messageRecvSocket.send(address, zmq.SNDMORE)
                    self.messageRecvSocket.send('', zmq.SNDMORE)
                    self.messageRecvSocket.send('OK')
                    if msg['name'] == 'ping': continue
                    self.messageSocketPub.send(msg.serialize())
            except zmq.ZMQError:
                self.connect()
        self._closeSockets()
        print 'Exiting work'

    def checkMsgQueue(self):
        """
        Checks queue of saved messages and tries resending messages that has not been ack:ed.
        """
        print 'Starting checkMsgQueue'
        while not self.stopped():
            for msgID, (lastChecked, msg) in self.msgCollection.iteritems():
                if lastChecked + MSG_CHECK_INTERVAL <= time.time():
                    if self.verbose:
                        print 'Sending un-acked message again: %s' % msg['checksum']
                    self.messageSocketPub.send(msg.serialize())
                    self.msgCollection[msgID] = (time.time(), msg)
            time.sleep(1)
        print 'Exiting checkMsgQueue'

    def _flushSavedMessages(self):
        with open(self.persistFile, 'wb') as f:
            cPickle.dump(self.msgCollection, f)

    def signalHandler(self, signum, frame):
        self.stop()
    
    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mezzenger server')
    parser.add_argument('--bind', action='store', dest='bindAddress', help='Address to bind server on. Default is 127.0.0.1.')
    parser.add_argument('--pubPort', action='store', dest='pubPort', type=int, help='Port for outgoing messages. Default is 7201.')
    parser.add_argument('--recvPort', action='store', dest='recvPort', type=int, help='Port for incoming messages. Default is 7202.')
    #parser.add_argument('--msgQueueMax', action='store', dest='msgQueueMax', type=int,
    #                    help='Max length of saved messages queue')
    parser.add_argument('--persistFile', action='store', dest='persistFile',
                        help='Path to file where to save persistant messages. Default is None, (no persist file is used).')
    parser.add_argument('--verbose', action='store_true', dest='verbose', default=False, help='Print verbose messages. Default is False.')
    args = parser.parse_args()
    MezzengerServer(bindAddress=args.bindAddress, recvPort=args.recvPort, pubPort=args.pubPort,
                    persistFile=args.persistFile, verbose=args.verbose)
