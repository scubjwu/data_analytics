import pymongo
import json
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
import time
import multiprocessing as mp
import threading
import Queue

#connect to DB
client = MongoClient()

#get database instance
db = client.test

#get collection instance
collection = db.t_NYC

"""
#query
for res in collection.find({"id_str":"636433425336037376"}):
	print(dumps(res))
"""

res = collection.find_one({"id_str":"636433425336037376"})
res_json = dumps(res)

user_info = res['user']
print(user_info['id_str'])

def worker(arg, q):
	print 'worker: ', arg
	q.put(str(arg))

def listener(q):
#TODO
	while 1:
		m = q.get()
		if m == 'kill':
			break
		print 'listener: ', m

class PrintThread(threading.Thread):
	def __init__(self, queue):
		threading.Thread.__init__(self)
		self.queue = queue

	def print_res(self, res):
		print res

	def run(self):
		while True:
			result = self.queue.get()
			self.print_res(result)
			self.queue.task_done()

class ProcessThread(threading.Thread):
	def __init__(self, in_queue, out_queue):
		threading.Thread.__init__(self)
		self.in_queue = in_queue
		self.out_queue = out_queue

	def process(self, data):
		lst = []
		for document in data:
			lst.extend(document)
		return lst

	def run(self):
		while True:
			m = self.in_queue.get()
			result = self.process(m)
			self.out_queue.put(result)
			self.in_queue.task_done()

cursors = collection.parallel_scan(4)
d_queue = Queue.Queue()
res_queue = Queue.Queue()

for i in cursors:
	t = ProcessThread(d_queue, res_queue)
	t.setDaemon(True)
	t.start()

t = PrintThread(res_queue)
t.setDaemon(True)
t.start()

for cursor in cursors:
	d_queue.put(cursor)

d_queue.join()
res_queue.join()

def process_cursor(cursor):
	for document in cursor:
		print document

def main():
	manager = mp.Manager()
	q = manager.Queue()
	pool = mp.Pool(mp.cpu_count() + 2)

	watcher = pool.apply_async(listener, (q,))

	jobs = []
	for cursor in range(10):
		job = pool.apply_async(worker, (cursor, q))
		jobs.append(job)

	for job in jobs:
		job.get()

	q.put('kill')
	pool.close()
	pool.join()

if __name__ == "__main__":
	main()
