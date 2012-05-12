#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (c) 2012 Gustav Arngården
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import cPickle
import time
import hashlib

class Message:
    def __init__(self, name, payload='', checksum='', ack=0):
        self.name = name
        self.payload = payload
        self.ack = ack
        self.timestamp = time.time()
        self.checksum = checksum or self.get_checksum()

    def __getitem__(self, key):
        return getattr(self, key)

    def __str__(self):
        return '\n%s\nName: %s\nAck: %s\nChecksum: %s\nPayload: %s\n%s\n' % ('='*10, self.name, self.ack,
                                                                       self.checksum, self.payload, '='*10)

    def get_checksum(self):
        return hashlib.md5('%s%s%s' % (self.name, self.payload, self.timestamp)).hexdigest()

    def serialize(self):
        return '%s|%s' % (self.name, cPickle.dumps(self.__dict__))


def parse(pickled_msg):
    """ Parse given message, returning Message-object
    """
    _, pickled_msg = pickled_msg.split('|', 1)
    s = cPickle.loads(pickled_msg)
    return Message(s['name'], s['payload'], s.get('checksum', ''), s['ack'])
