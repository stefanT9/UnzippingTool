#!/usr/bin/python
import argparse
import logging
import multiprocessing
import os
import shutil
import signal
import thread
import threading
import time
import zipfile

import humanfriendly
import ThreadLock
import walk

from collections import deque
clear = lambda: os.system('clear')



def timeout(signum, frame):
	with threading.Lock():
		logging.debug(str(time.ctime(time.time())+' Program reached global timeout some tasks may have been let unfinished'))
		logging.info('End of session \n\n')
	exit(1)


def get_all_zips(directory):
	try:
		for file_name in os.listdir(directory):
			path=directory+"/"+file_name
			
			try:
				if (zipfile.is_zipfile(path) and ('.zip' in file_name)):
					with threading.Lock():
						logging.info(str(time.ctime(time.time())+' found zip '+path))
					all_zips.append(path)
			except LargeZipFile:
				with threading.Lock():
					logging.debug(str(time.ctime(time.time()))+' zip '+path+' is too big')
			except BadZipFile:
				with threading.Lock():
					logging.debug(str(time.ctime(time.time()))+' zip '+path+' is corrupted')

	except  OSError :
		with threading.Lock():
			logging.error(str(time.ctime(time.time())+' Invalid path'))
			print ("INVALID PATH")
			logging.info(str(time.ctime(time.time()))+'End of session \n\n')
		exit (1)
				
def unzip(source_filename):
	
	destination=source_filename.split('/')
	file_name=destination[-1][:-4]
	destination.remove(destination[-1])
	destination="/".join(destination)
	destination=destination+'/'+file_name

	try:
		with zipfile.ZipFile(source_filename) as zf:
			zf.extractall(destination)
	except:
		logging.error(str(time.ctime(time.time())+' unable to extract '+source_filename))
		pass

def print_threads_info(Threads):
	clear()
	print time.ctime(time.time())
	with threading.Lock():
		for thread in Threads:
			thread.get_info()
		
class WorkerThread (threading.Thread):
	def __init__(self, threadID, name,ttime):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.is_busy=False
		self.task=''
		self.kill_switch=False
		self.nr_of_processed_files=0
		self.fails=0
		self.daemon=1
		self.zip_timeout=ttime
		self.cleaning=False

	def get_info(self):
		if self.cleaning==True:
			print self.name+' Cleaning after '+self.task.split('/')[-1]\
			+' #fails= '+str(self.fails)\
			+' #processed files= '+str(self.nr_of_processed_files)
		elif self.is_busy==True:
			print self.name+' unziping '+self.task.split('/')[-1]+' size: '\
			+str(humanfriendly.format_size(os.path.getsize(self.task)))\
			+' #fails= '+str(self.fails)\
			+' #processed files= '+str(self.nr_of_processed_files)
		else:
			print self.name+' Idle'+' #fails= '+str(self.fails)\
			+' #processed files= '+str(self.nr_of_processed_files)
	def run(self):
		with threading.Lock():
			logging.info(str(time.ctime(time.time())+' Thread '+self.name+' started'))
		
		while self.kill_switch==False:
			if self.task!='':
				
				with threading.Lock():
					logging.debug(str(time.ctime(time.time())+' '+self.name+' starts unzipping '+self.task))
				
				unzip_proces= multiprocessing.Process(target=unzip,args=(self.task,))
				unzip_proces.daemon=True
				unzip_proces.start()

				unzip_proces.join(self.zip_timeout)
				if unzip_proces.is_alive():
					with threading.Lock():
						logging.debug(str(time.ctime(time.time())+' '+self.name+' Task '+self.task+' timed out'))
					
					unzip_proces.terminate()
					self.fails+=1
					self.cleaning=True
					shutil.rmtree(self.task[:-4])
					self.cleaning=False

				else:
					with threading.Lock():
						logging.debug(str(time.ctime(time.time())+' '+self.name+' Unzipping of '+self.task+' completed'))
				
				self.task=''
				self.is_busy=False

		with threading.Lock():
			logging.info(str(time.ctime(time.time())+' '+self.name+' killed'))

	def give_task(self,task):
		with threading.Lock():
			self.task=task
			self.is_busy=True
			self.nr_of_processed_files=self.nr_of_processed_files+1
			logging.info (str(time.ctime(time.time())) + ' Archive '+self.task+' of size '+str(humanfriendly.format_size(os.path.getsize(self.task)))+' given to '+self.name )

class ConsoleThread(threading.Thread):
	def __init__(self, threadID, name):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.is_busy=True
		self.fails=0
		self.kill_switch=False
		self.daemon=1

	def run(self):
		while self.kill_switch==False:
			print_threads_info(threads)
			time.sleep(1)
		
		with threading.Lock():
			logging.info(str(time.ctime(time.time())+' '+self.name+' killed'))

if __name__ =='__main__':

	threads=[]
	all_zips=deque()

	try:
		parser = argparse.ArgumentParser()
		parser.add_argument('-t'         , default=1                               , type=int , help=' numarul de threaduri dedicate dezarhivarii')
		parser.add_argument('--log'      , default=('default.log')                 , type=str , help=' nume log file')
		parser.add_argument('--timeout'  , default=5                               , type=int , help=' nr de secunde pana la timeout per zip')
		parser.add_argument('--gtimeout' , default=10                              , type=int , help=' nr de secunde pana la timeout global')
		parser.add_argument('dir'        , default='.'                             , type=str , help=' directorul de lucru')
		args = parser.parse_args()
	except:
		logging.error(str(time.ctime(time.time()))+' invalid arguments at input')
		logging.info(str(time.ctime(time.time()))+'End of session \n\n')
		exit(2)

	log_file=args.log
	no_of_threads=args.t
	thread_timeout=args.timeout
	global_timeout=args.gtimeout
	work_directory=args.dir
    
	signal.signal(signal.SIGALRM, timeout)
	signal.alarm(global_timeout)

	logging.basicConfig(filename=log_file,level=logging.DEBUG)
	
	logging.info(str(time.ctime(time.time())+' started new session with following arguments '+str(args)))
	
	get_all_zips(work_directory)

	printing_thread=ConsoleThread(0,'printing_thread')
	n_processes=0
	
	for i in range(no_of_threads):
		try:
			threads.append(WorkerThread(i+1, "thread-"+str(i+1),thread_timeout))
		except:
			with threading.Lock():
				logging.critical(str(time.ctime(time.time())+' Unable to start thread, EXITING'))
				exit(3)
				logging.info(str(time.ctime(time.time()))+'End of session \n\n')


	
	for thread in threads:
		thread.start()
		
	printing_thread.start()
	while all_zips:
		for thread in threads:
			if (thread.is_busy==False):
				try:
					with threading.Lock():
						thread.give_task(all_zips.popleft())
				except:
					with threading.Lock():
						logging.error(str(time.ctime(time.time()))+' '+thread.name+' Error when giving task to thread')
					pass
	
	still_cleaning=True
	while still_cleaning:
		still_cleaning=False
		for thread in threads:
			if(thread.is_busy==True):
				still_cleaning=True
	
	time.sleep(1)
	printing_thread.kill_switch=True
	for thread in threading.enumerate():
		thread.kill_switch=True

	time.sleep(1)
	with threading.Lock():
		logging.info(str(time.ctime(time.time()))+'End of session \n\n')
	
