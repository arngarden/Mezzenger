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
    def __init__(self, name, msg='', checksum='', ack=0):
        self.name = name
        self.msg = msg
        self.ack = ack
        self.timestamp = time.time()
        self.checksum = checksum or (self.getChecksum() if ack > 0 else '')

    def __getitem__(self, key):
        return getattr(self, key)

    def __str__(self):
        return '\n%s\nName: %s\nAck: %s\nChecksum: %s\nMsg: %s\n%s\n' % ('='*10, self.name, self.ack,
                                                                       self.checksum, self.msg, '='*10)

    def getChecksum(self):
        return hashlib.md5('%s%s%s' % (self.name, self.msg, self.timestamp)).hexdigest()

    def serialize(self):
        return '%s|%s' % (self.name, cPickle.dumps(self.__dict__))


def parse(pickledMsg):
    _, pickledMsg = pickledMsg.split('|', 1)
    s = cPickle.loads(pickledMsg)
    #print s
    return Message(s['name'], s['msg'], s.get('checksum', ''), s['ack'])
