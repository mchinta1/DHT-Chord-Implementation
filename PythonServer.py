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
import glob
import sys
import socket
import hashlib
sys.path.append('gen-py')
#sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException,RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class My_Transport():
	def __init__(self,ip,port):
		 self.transport = TSocket.TSocket(ip, port)
		 self.transport = TTransport.TBufferedTransport(self.transport)
		 self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
		 self.client = FileStore.Client(self.protocol)
	def connect(self):
		 self.transport.open()
	def close(self):
		self.transport.close()


class FileStoreHandler:
	def __init__(self,port):
		print("Intialized")
		self.file_lookup = {}
		self.finger_table= []
		#self.hostname = socket.gethostname()
		IP = socket.gethostbyname('localhost')
		self.NodeID = NodeID()
		self.NodeID.ip = IP
		self.NodeID.port = int(port)
		sha256 = hashlib.sha256()
		sha256.update((IP+':'+port).encode('utf-8'))
		#id = hashlib.sha256(IP+':'+port)
		#self.NodeID.id = id.hexdigest()
		self.NodeID.id = sha256.hexdigest()
		print(self.NodeID)

	def writeFile(self,rFile):
		print("In Write File - ",rFile)
		file_id = rFile.meta.owner + ':' +rFile.meta.filename
		sha256 = hashlib.sha256()
		sha256.update(file_id.encode('utf-8'))
		file_id = sha256.hexdigest()
		try:
			if file_id in self.file_lookup:
				count = self.file_lookup[file_id].meta.version
				count +=1
				sha256.update((rFile.content).encode('utf-8'))
				rFile.meta.contentHash = sha256.hexdigest()
				self.file_lookup[file_id] = rFile
				(self.file_lookup[file_id]).meta.version = count
			else:
				target = self.findSucc(file_id)
				if int(target.id,16) == int(self.NodeID.id,16):
					rFile.meta.version = 1
					sha256.update((rFile.content).encode('utf-8'))
					rFile.meta.contentHash = sha256.hexdigest()
					print(rFile)
					self.file_lookup[file_id] = rFile
					print('File Written to Serve Sucessfully')
				else:
					print('File doesnt belong to this server')
					raise SystemException

		except SystemException as e:
			return e.message




	def readFile(self, filename,owner):
		print('Filename & owner - ',filename,owner)
		rFile = RFile()
		file_id = owner + ':' + filename
		sha256 = hashlib.sha256()
		sha256.update(file_id.encode('utf-8'))
		file_id = sha256.hexdigest()
		if file_id in self.file_lookup:
			rFile =  self.file_lookup[file_id]
			return rFile
		else:
			raise SystemException

	def setFingertable(self, node_list):
        	#print('Node List',node_list)
		self.finger_table = node_list

		for x in range(256):
			self.finger_table[x].id = hex(int(self.finger_table[x].id,16))
		print(self.finger_table)
	def findSucc(self,key):
		print('find succ-',key)
		key = int(key,16)
		succ = self.getNodeSucc()
		if key == int(self.NodeID.id,16):
			x =  self.NodeID
		elif key > int(self.NodeID.id,16) and key < int(succ.id,16):
			return succ
		else:
			target = self.findPred(hex(key))
			if int(target.id,16) != int(self.NodeID.id,16):
				t = My_Transport(target.ip, target.port)
				t.connect()
				x = t.client.getNodeSucc()
				t.close()
			else:
				x = self.getNodeSucc()
		return x
	def findPred(self,key):
		'''if key == self.NodeID.id:
			return self.NodeID'''
		key = int(key,16)
		#key = hex(key)
		#key = key[2:]
		#print(key)
		if key > 2**256:
			k = key % (2 ** 256)
			#key = hex(k)
			print('find pred-{0}'.format(key))
			#key = hex(long(key,16) % (2 ** 255))[2:0]
		x = self.NodeID
		succ = self.getNodeSucc()
		if len(self.finger_table) == 0:
			return SystemException
		if(int(self.NodeID.id,16) > int(succ.id,16)):
			diff1 = key - int(succ.id,16)
			diff2 = key - int(self.NodeID.id,16)
			if(diff1<=0) or (diff2 >0):
				x = self.NodeID
			else:
				target = self.close_preceding_finger(key)
				t = My_Transport(target.ip,target.port)
				t.connect()
				x = t.client.findPred(hex(key))
				t.close()
		else:
			if(key > int(self.NodeID.id,16)):
				if(key <= int(succ.id,16)):
					x = self.NodeID
				else:
					target = self.close_preceding_finger(key)
					t = My_Transport(target.ip, target.port)
					t.connect()
					x = t.client.findPred(hex(key))
					t.close()
			else:
				target = self.close_preceding_finger(key)
				t = My_Transport(target.ip, target.port)
				t.connect()
				x = t.client.findPred(hex(key))
				t.close()
		return x
	def getNodeSucc(self):
		print('get node succ')
		return self.finger_table[0]

	def close_preceding_finger(self, id):
		print('in close')
		for i in range(255, -1, -1):
			k = self.finger_table[i]
			if int(self.NodeID.id,16) < id:
				if (int(k.id,16) > int(self.NodeID.id,16) and int(k.id,16) < id):
					return k
			else:
				if (int(k.id,16) > int(self.NodeID.id,16) or int(k.id,16) < id):
					return k
		return self.NodeID
if __name__ == '__main__':
	print(sys.argv[1])
	handler = FileStoreHandler(sys.argv[1])
	processor = FileStore.Processor(handler)
	transport = TSocket.TServerSocket(port = sys.argv[1])
	tfactory = TTransport.TBufferedTransportFactory()
	pfactory = TBinaryProtocol.TBinaryProtocolFactory()
	server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)
	print('Starting the server on port...{0}'.format(sys.argv[1]))
	server.serve()
	print('done.')
