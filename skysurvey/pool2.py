#!/usr/bin/env python

from multiprocessing import Process, Queue, cpu_count
from collections import defaultdict
import cPickle as pickle
import os
import sys
import tempfile
import time

def _worker(qin, qout):
	# Just dispatch the call to the target function
	for mapper, i, args in iter(qin.get, 'EXIT'):
		qout.put((i, mapper(*args)))

def _reduce_from_pickled(kw, pkl, reducer, args):
	# open the piclke jar, load the objects, pass them on to the
	# actual reducer
	d = open(pkl, 'rb')
	k, vd = kw
	va = []
	for offs in vd:
		d.seek(offs)
		obj = pickle.load(d)
		va.append(obj)
	d.close()

	return reducer((k, va), *args)

def progress_default(stage, step, input, index, result):
	self = progress_default

	if  step == 'begin':
		if '__len__' in dir(input):
			self.dispatch = progress_pct
		else:
			self.dispatch = progress_dots

	self.dispatch(stage, step, input, index, result)

def progress_pct(stage, step, input, index, result):
	self = progress_pct

	# Record the first 'begin' stage as the endstage
	if step == 'begin' and 't0' not in dir(self):
		self.t0 = time.time()
		self.endstage = stage
		self.head = 'm/r' if stage == 'mapreduce' else 'm'

	if step == 'begin' and (stage == 'map' or stage == 'reduce'):
			self.len = len(input)
			self.at = 0
			self.pct = 5

			if   stage == 'map':
				sys.stderr.write("[%s (%d elem): " % (self.head, self.len)),
				self.sign = ':'
			elif stage == "reduce":
				sys.stderr.write('|'),
				self.sign = '+'
	elif step == 'step':
		self.at = self.at + 1
		pct = 100. * self.at / self.len
		while self.pct <= pct:
			sys.stderr.write(self.sign)
			self.pct = self.pct + 5
	elif step == 'end':
		if stage == self.endstage:
			t = time.time() - self.t0
			sys.stderr.write(']  %.2f sec\n' % t)
			del self.t0

def progress_dots(stage, step, input, index, result):
	if step == 'begin':
		if   stage == 'map':
			sys.stderr.write("[map: "),
		elif stage == "reduce":
			sys.stderr.write(' [reduce: '),
	elif step == 'step':
		sys.stderr.write("."),
	elif step == 'end':
		sys.stderr.write(']')

def progress_pass(stage, step, input, index, result):
	pass

class Pool:
	qin = None
	qout = None
	ps = []
	DEBUG = False
	#DEBUG = True

	def __init__(self, nworkers = None, DEBUG=False):
	        if DEBUG:
	            self.DEBUG=True
	            return

		if nworkers == None:
			nworkers = cpu_count()

		self.qin = Queue()
		self.qout = Queue(nworkers*2)
		self.ps = [ Process(target=_worker, args=(self.qin, self.qout)) for i in xrange(nworkers) ]

		for p in self.ps:
			p.daemon = True
			p.start()

	def imap_unordered(self, input, mapper, mapper_args=(), return_index=False, progress_callback=None, progress_callback_stage='map'):
		""" Execute in parallel a callable <mapper> on all values of
		    iterable <input>, ensuring that no more than ~nworkers
		    results are pending in the output queue """
		if progress_callback == None:
			progress_callback = progress_default;

		progress_callback(progress_callback_stage, 'begin', input, None, None)

		if not self.DEBUG:
			i = -1
			for (i, val) in enumerate(input):
				self.qin.put( (mapper, i, (val,) + mapper_args) )
			n = i + 1

			# yield the outputs
			for val in xrange(n):
				(i, result) = self.qout.get()
				progress_callback(progress_callback_stage, 'step', input, i, result)
				if return_index:
					yield (i, result)
				else:
					yield result
		else:
			for (i, val) in enumerate(input):
				result = mapper(val, *mapper_args)
				progress_callback(progress_callback_stage, 'step', input, i, result)
				if return_index:
					yield (i, result)
				else:
					yield result

		progress_callback(progress_callback_stage, 'end', input, None, None)

	def imap_reduce(self, input, mapper, reducer, mapper_args=(), reducer_args=(), progress_callback=None):
		""" A poor-man's map-reduce implementation.
		
		    Calls the mapper for each value in the <input> iterable. 
		    The mapper shall return a list of key/value pairs as a
		    result.  Once all mappers have run, reducers will be
		    called with a key, and a list of values associated with
		    that key, once for each key.  The reducer's return
		    values are yielded to the user.

		    Input: Any iterable
		    Output: Iterable
		    
		    Notes:
		    	- mapper must return a dictionary of (key, value) pairs
		    	- reducer must expect a (key, value) pair as the first
		    	  argument, where the value will be an iterable
		"""

		if progress_callback == None:
			progress_callback = progress_default
		
		progress_callback('mapreduce', 'begin', input, None, None)

		# Map step
		mresult = defaultdict(list)
		for r in self.imap_unordered(input, mapper, mapper_args, progress_callback=progress_callback, progress_callback_stage='map'):
			for (k, v) in r:
				mresult[k].append(v)

		# Reduce step
		for r in self.imap_unordered(mresult.items(), reducer, reducer_args, progress_callback=progress_callback, progress_callback_stage='reduce'):
			if len(r) > 2:
				print r
			yield r

		if progress_callback != None:
			progress_callback('mapreduce', 'end', None, None, None)

	def imap_reduce_big(self, input, mapper, reducer, mapper_args=(), reducer_args=(), progress_callback=None):
		#
		# Notes: same interface as imap_reduce, except that the outputs of
		#        map phase are assumed to be large and are cached on 
		#        the disk using cPickle. The (key->index on disk) mappings
		#        are still held in memory, so make sure those don't grow
		#        too large.
		#

		if progress_callback == None:
			progress_callback = progress_default
		
		progress_callback('mapreduce', 'begin', input, None, None)

		# Map step
		d = tempfile.NamedTemporaryFile(mode='wb', prefix='mapresults-', suffix='.pkl', delete=False)
		mresult = defaultdict(list)
		for r in self.imap_unordered(input, mapper, mapper_args, progress_callback=progress_callback, progress_callback_stage='map'):
			for (k, v) in r:
				mresult[k].append(d.tell())
				pickle.dump(v, d, -1)
		d.close()

		# Reduce step
		for r in self.imap_unordered(mresult.iteritems(), _reduce_from_pickled, (d.name, reducer, reducer_args), progress_callback=progress_callback, progress_callback_stage='reduce'):
			yield r

		os.unlink(d.name)

		if progress_callback != None:
			progress_callback('mapreduce', 'end', None, None, None)
