#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2012 Gustav Arngården
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import os
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
    def __init__(self, bind_address=None, pub_port=None, recv_port=None,
                 persist_file=None, verbose=False):
        """ Init server.
        Args:
        - bind_address: Address to bind to
        - pub_port: Port for sending messages
        - recv_port: Port for receiving messages
        - persist_file: File for saving messages that are waiting for ack
        - verbose: Print verbose info if True
        
        """
        self.verbose = verbose
        self.pub_port = pub_port or DEFAULT_PUB_PORT
        self.recv_port = recv_port or DEFAULT_RECV_PORT
        self.bind_address = bind_address or DEFAULT_SERVER
        self.message_recv_socket = None
        self.message_socket_pub = None
        try:
            self.context = zmq.Context()
            self.poller = zmq.Poller()
            self.connect()
        except zmq.ZMQError:
            raise MezzengerServerException('Could not connect sockets')
        self.persist_file = persist_file or ''
        if self.verbose:
            print 'Using persist_file: %s' % self.persist_file
        if os.path.isfile(self.persist_file):
            try:
                with open(self.persist_file, 'rb') as f:
                    self.msg_collection = cPickle.load(f)
                print 'Found old msg_collection with %s messages' % len(self.msg_collection)
            except IOError:
                raise MezzengerServerException('Could not open persist file: %s' % self.persist_file)
        else:
            self.msg_collection = {}
        self._stop = threading.Event()
        signal.signal(signal.SIGINT, self.signal_handler)
        work_thread = threading.Thread(target=self.work)
        work_thread.start()
        check_msg_thread = threading.Thread(target=self.check_msg_queue)
        check_msg_thread.start()
        while True:
            if self.stopped():
                break
            time.sleep(2)

        
    def connect(self):
        """ Connect sockets.
        """
        print 'Binding sockets'
        if self.message_recv_socket:
            self.poller.unregister(self.message_recv_socket)
            self.message_recv_socket.setsockopt(zmq.LINGER, 0)
            self.message_recv_socket.close()
        self.message_recv_socket = self.context.socket(zmq.ROUTER)
        self.message_recv_socket.bind('tcp://%s:%s' % (self.bind_address, self.recv_port))
        self.poller.register(self.message_recv_socket, zmq.POLLIN)
        if self.verbose:
            print 'Bound recv-socket at tcp://%s:%s' % (self.bind_address, self.recv_port)
        if self.message_socket_pub:
            self.message_socket_pub.close()
        self.message_socket_pub = self.context.socket(zmq.PUB)
        self.message_socket_pub.bind('tcp://%s:%s' % (self.bind_address, self.pub_port))
        if self.verbose:
            print 'Bound pub-socket at tcp://%s:%s' % (self.bind_address, self.pub_port)

    def _close_sockets(self):
        """ Close sockets.
        """
        self.message_recv_socket.setsockopt(zmq.LINGER, 0)
        self.message_recv_socket.close()
        self.message_socket_pub.close()
        
    def work(self):
        """ Start main loop.
        Listen for incoming messages and resend them to subscribers.
        
        """
        print 'Starting work'
        while not self.stopped():
            try:
                socks = dict(self.poller.poll(5))
                if socks.get(self.message_recv_socket) == zmq.POLLIN:
                    # received message from client
                    address = self.message_recv_socket.recv()
                    _ = self.message_recv_socket.recv()
                    msg = self.message_recv_socket.recv()            
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
                            self.msg_collection.pop(msg['payload'])
                            if self.persist_file:
                                self._flush_saved_messages()
                        except KeyError:
                            pass
                    if int(msg['ack']) > 0:
                        # save message and make sure server receive ack
                        if self.verbose:
                            print 'Delivery of message with ack: %s' % msg['checksum']
                        self.msg_collection[msg['checksum']] = (time.time(), msg)
                        if self.persist_file:
                            self._flush_saved_messages()
                    self.message_recv_socket.send(address, zmq.SNDMORE)
                    self.message_recv_socket.send('', zmq.SNDMORE)
                    self.message_recv_socket.send('OK')
                    if msg['name'] == 'ping':
                        continue
                    self.message_socket_pub.send(msg.serialize())
            except zmq.ZMQError:
                self.connect()
        self._close_sockets()
        print 'Exiting work'

    def check_msg_queue(self):
        """
        Checks queue of saved messages and tries resending messages that has not been ack:ed.
        """
        print 'Starting check_msg_queue'
        while not self.stopped():
            for msg_id, (last_checked, msg) in self.msg_collection.iteritems():
                if last_checked + MSG_CHECK_INTERVAL <= time.time():
                    if self.verbose:
                        print 'Sending un-acked message again: %s' % msg['checksum']
                    self.message_socket_pub.send(msg.serialize())
                    self.msg_collection[msg_id] = (time.time(), msg)
            time.sleep(1)
        print 'Exiting check_msg_queue'

    def _flush_saved_messages(self):
        with open(self.persist_file, 'wb') as f:
            cPickle.dump(self.msg_collection, f)

    def signal_handler(self, signum, frame):
        self.stop()
    
    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Mezzenger server')
    parser.add_argument('--bind', action='store', dest='bind_address', help='Address to bind server on. Default is 127.0.0.1.')
    parser.add_argument('--pub-port', action='store', dest='pub_port', type=int, help='Port for outgoing messages. Default is 7201.')
    parser.add_argument('--recv-port', action='store', dest='recv_port', type=int, help='Port for incoming messages. Default is 7202.')
    #parser.add_argument('--msgQueueMax', action='store', dest='msgQueueMax', type=int,
    #                    help='Max length of saved messages queue')
    parser.add_argument('--persist-file', action='store', dest='persist_file',
                        help='Path to file where to save persistant messages. Default is None, (no persist file is used).')
    parser.add_argument('--verbose', action='store_true', dest='verbose', default=False, help='Print verbose messages. Default is False.')
    args = parser.parse_args()
    MezzengerServer(bind_address=args.bind_address, recv_port=args.recv_port, pub_port=args.pub_port,
                    persist_file=args.persist_file, verbose=args.verbose)
