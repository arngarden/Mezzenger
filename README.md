# Mezzenger
### Simple messaging/event system based on ZeroMQ

Mezzenger is a simple messaging system written in Python using ZeroMQ as backend.
It supports sending messages, (really any kind of Python-object that can be pickled), from one or more publishers to one or more subscribers through a server.


## Quick start
### Server
```
python mezzengerServer.py --bind='127.0.0.1'
```
Start a mezzengerServer that binds on IP 127.0.0.1.

### Client 1 (publisher)
'''python
import time, random
import mezzengerClient
mc = mezzengerClient.Mezzenger(serverAddress='127.0.0.1', autoStart=False)
people = ['John', 'David', 'Lucy']
while True:
  mc.sendMessage('greetings', 'Hello %s!' % random.choice(people))
  time.sleep(3)
'''
Create mezzenger client that connects to the server on 127.0.0.1. Since this client only sends messages we can set autoStart=False to prevent it from starting the thread that listens to new messages.
The client then starts sending random greetings to every other client that listens to the channel 'greetings'.

### Client 2 (subscriber)
'''python
import mezzengerClient

def callback(msg):
  print msg
  
mc = mezzengerClient.Mezzenger(serverAddress='127.0.0.1')
mc.subscribe('greetings', callback)
'''
Start mezzengerClient that subscribes to all 'greetings' and prints the greeting.


## The server - mezzengerServer.py
All messages are sent through the MezzengerServer which of course needs to run on a server that is accessible to all clients.
The server is easily started with 'python mezzengerServer.py'. The following command line arguments are available:
'''
usage: mezzengerServer.py [-h] [--bind BINDADDRESS] [--pubPort PUBPORT]
                          [--recvPort RECVPORT] [--persistFile PERSISTFILE]
                          [--debug]

Mezzenger server

optional arguments:
  -h, --help            show this help message and exit
  --bind BINDADDRESS    Address to bind server on. Default is 127.0.0.1.
  --pubPort PUBPORT     Port for outgoing messages. Default is 7201.
  --recvPort RECVPORT   Port for incoming messages. Default is 7202.
  --persistFile PERSISTFILE
                        Path to file where to save persistant messages.
                        Default is None, (no persist file is used).
  --debug               Print debug messages. Default is False.
'''
The parameters bind, pubPort and recvPort are used for determining which address and ports the server uses for receiving and sending messages.
Messages that are waiting to be acknowledged by a subscriber are by default only saved in memory. If we would like these messages to also be persisted to a file we can set a file name in the parameter persistFile.

## The client - mezzengerClient.py
The client is used for sending and subscribing to messages. The Mezzenger-class has the following arguments:
'''
serverAddress - Address that the server is bound to. Default is 127.0.0.1.
sendPort - Port for outgoing messages. Default is 7202.
subPort - Port for incoming messages. Default is 7201.
sendTimeout - Number of seconds the client tries sending message to server on each retry. Default is 2 seconds.
connectionRetries - Number of connection retries that is made if the server is unresponsive before throwing exception.
autoStart - If False, the listening thread is not started, useful if client is only used for sending messages. Default is True.
verbose - If True, status information is printed.
'''
You send messages by calling the sendEvent method:
'''python
def sendMessage(self, msgName, msg, ack=0)
'''
'msgName' is the name of the message that is used by subscribers to filter it.
'msg' is the message payload.
'ack' denotes whether the server should demand an acknowledgement from a client that the message has been received.

For subscribing to a message you use the subscribe method:
'''python
def subscribe(self, msgName, callback)
'''
'msgName' is the name of the message to subscribe to.
'callback' is a function that is called when a message is received. The function should accept only one argument, the message payload.

## Sending messages
A message has two parts, a name and a payload. The name should be an ordinary string (note that the pipe-symbol "|" can not be used in a message name). The payload can be any Python object that can be pickled.
To send a message with name 'greeting' and payload 'Hello Gustav!' we write:
'''python
mc = mezzengerClient.Mezzenger()
mc.sendMessage('greeting', 'Hello Gustav!')
'''
This message reaches all subscribers that are currently subscribing to messages with the name 'greeting'. If there are no such subscribers, the message is simply discarded.





