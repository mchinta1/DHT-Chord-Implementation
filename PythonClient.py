#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import glob
sys.path.append('gen-py')
#sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])
import hashlib
from chord import FileStore
from chord.ttypes import SystemException,RFileMetadata, RFile, NodeID

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def main():
    # Make socket
    port = sys.argv[1]
    transport = TSocket.TSocket('localhost', port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect!
    transport.open()

    rFile =  RFile()
    #print(rFile)
    rFile.meta = RFileMetadata()
    rFile.meta.filename = 'test.txt'
    rFile.content = 'test'
    client.writeFile(rFile)
    print('ping()')

    s = client.readFile('test.txt','guest')
    print(s.content)
    print(s.meta.version)

    #work = Work()

    #work.op = Operation.DIVIDE
    #work.num1 = 1
    #work.num2 = 0

    '''try:
        quotient = client.calculate(1, work)
        print('Whoa? You know how to divide by zero?')
        print('FYI the answer is %d' % quotient)
    except InvalidOperation as e:
        print('InvalidOperation: %r' % e)

    work.op = Operation.SUBTRACT
    work.num1 = 15
    work.num2 = 10
    '''
    k = client.getNodeSucc()
    print('getNodeSucc',k)

    client.findSucc('123456')
    #l = client.findPred('8f019af015bab34f148262235d11a541d607adad3e1db9d2283cbf283a4e991b')
   # l = client.findPred('82b9e6b1548ad0837abe6ff674d1d3e982a2038442a1059f595d95962627f827') #91
    l = client.findPred('fef33d8355d29002c802820e9d371b9d8c3c7a14eb072ac4a5090556a4285013')  # 92
    print('findPred93-', l)
    l = client.findPred('e318e4cd5089903a714f2d5585fb1c70bf1ed0af4fc8fea5b3775a5a687fd040')  # 90
    print('findPred90-', l)
    l = client.findPred('b2bd42182ac6a8872c76db866738d47e04f66a3d2cf5c359962008baa052dbc4')  # 92
    print('findPred92-', l)


    #l = client.findPred('72b9e6b1548ad0837abe6ff674d1d3e982a2038442a1059f595d95962627f827')
    #l = client.findPred('305ba2b525c2e7b344d851cf1ec247f2ba28667222d33b4c1d57519fc82cf418')

    #print(client.getNodeSucc())


    # Close!
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
