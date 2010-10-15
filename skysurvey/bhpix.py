#!/usr/bin/env python

from math import *
import numpy as np

def autovectorized(f):
	"""Function decorator to do vectorization only as necessary.
	vectorized functions fail for scalar inputs.
	Taken from http://mail.scipy.org/pipermail/numpy-discussion/2006-July/009480.html
	"""
	def wrapper(l, b):
		if type(l) == np.ndarray or type(b) == np.ndarray:
			return np.vectorize(f)(l, b)
		return f(l, b)
	return wrapper

@autovectorized
def proj_healpix(l, b):
	z   = cos(radians(90.-b))
	phi = radians(fmod(fmod(l, 360.)+360, 360)) # wrap to [0,360)

	phit = fmod(phi, .5*pi)
	sigz = (2 - sqrt(3*(1-z))) if z > 0 else -(2 - sqrt(3*(1+z)))

	x = phi		if abs(z) < 2./3 else phi - (abs(sigz) - 1) * (phit - 0.25 * pi)
	y = 3./8*pi*z	if abs(z) < 2./3 else 0.25 * pi * sigz

	return (x, y)

_c = [-1, -1,  1, 1]
_s = [ 1, -1, -1, 1]

@autovectorized
def proj_bhealpix(l, b):
	(hxx, hyy) = proj_healpix(l, b)

	l = int(hxx / (0.5*pi))
	h1x = (hxx - pi/2*(0.5 + l))
	h1y = hyy - pi/2

	h1 = (h1x * _c[l] - h1y * _s[l]) / pi
	h2 = (h1x * _s[l] + h1y * _c[l]) / pi

	return (h1, h2)

@autovectorized
def deproj_healpix(x, y):
	# Equations from Calabretta and Roukema, MNRAS, 381, 865
	K = 3; H = 4;

	if abs(y) <= 0.5*pi * (K-1)/H:
		# Equatorial regions
		l = x
		b = asin(y*H / (0.5*pi*K))
	else:
		# Polar regions
		w = 1 if fmod(K, 2) == 1 else 0
		s = 0.5*(K+1) - abs(y*H)/pi
		xc = -pi + (2.*floor( (x + pi)*H/(2*pi) + 0.5*(1-w)) + w)*pi/H
		l = xc + (x - xc) / s
		b = asin(1 - s*s/K) * (1 if y > 0 else -1)

	return degrees(l), degrees(b)

@autovectorized
def deproj_bhealpix(x, y):
	""" Deproject from butterfly-HealPix to lon, lat
	"""
	# Compute to which of the four healpix slices
	# this point belongs to
	l = atan2(y, x)*180/pi
	l = l if l > 0 else l + 360
	l = int(l // 90)

	h1x = pi/2. * (_c[l]*x + _s[l]*y)
	h1y = pi/2. * (_c[l]*y - _s[l]*x)
	
	hxx = h1x + pi/2.*(0.5 + l)
	hyy = h1y + pi/2.

	return deproj_healpix(hxx, hyy)

def ij_center(i, j, level):
	dx = pix_size(level)
	return ((i + 0.5)*dx, (j + 0.5)*dx)

def xy_to_ij(x, y, level):
	dx = pix_size(level)
	return (x // dx, y // dx)

def xy_center(x, y, level):
	(i, j) = xy_to_ij(x, y, level)
	return ij_center(i, j, level)

def pix_size(level):
	return 2.**(1-level)

def pix_idx(x, y, level):
	i, j = xy_to_ij(x, y, level)
	wh = nside(level)
	i = i + wh/2; j = j + wh/2;
	return j * wh + i

def nside(level):
	''' Return the number of pixels on the side for a given split level. '''
	return 2**level

def get_subpath(x, y, level):
	(cx, cy) = xy_center(x, y, level)
	return "%+g%+g%+g" % (cx, cy, pix_size(level))

def get_pixel_level(x, y):
	# deduce the level, assuming (x,y) are the center of a pixel
	for levx in xrange(32):
		p = x*(1<<levx)
		if int(p) == p: break;

	for levy in xrange(32):
		p = y*(1<<levy)
		if int(p) == p: break;

	if(levx != levy or levx == 31):
		raise Exception('Invalid pixel center coordinate ' + str((x,y)))

	return levx

def get_path(x, y, level = None):
	if level == None:
		level = get_pixel_level(x, y)

	path = '';
	for lev in xrange(1, level+1):
		(i, j) = xy_center(x, y, level)
		path = path + get_subpath(x, y, lev) + '/'
	return path[:-1]

def testview():
	vl = np.arange(0., 360., 5.)
	vb = np.arange(-90., 90., 5.)
	(l, b) = zip(*[ (l, b) for b in vb for l in vl ])
	(x, y) = toBHealpix(np.array(l), np.array(b))

	scatter(x, y); show();

def testpix():
	#x = -0.5; y = 0.721; k = 6;
	k = 6;
	for ctr in xrange(100000):
		(x, y) = ran.uniform(-1, 1), ran.uniform(-1, 1)
		i, j = xy_center(x, y, k)
		ns   = nside(k)
		dx   = pix_size(k)
		if not (i-dx/2 <= x < i+dx/2) or not (j-dx/2 <= y < j+dx/2):
			print x, y, '->', i, j, ns, dx, get_path(x, y, k)
			raise Exception('Bug bug bug -- test failed.!!')
	print x, y, '->', i, j, ns, dx, get_path(x, y, k)
	print "Passed."

################################################################
def map_to_valid_pixels(x, y, dx, assume_adjacent=True):
	if not assume_adjacent:
		raise Exception('Not implemented.')

	# Map into a pixel within range
	ax = fabs(x)
	ay = fabs(y)
	if ax > 1 or ay > 1:
		# Fallen off the rectangle
		if y > 1.:
			if x > 1:
				pix = (-(x-dx), -(y-dx))
			elif x < -1:
				pix = (-(x+dx), -(y-dx))
			else:
				pix = (-x, y-dx)
		elif y < -1.:
			if x > 1:
				pix = (-(x-dx), -(y+dx))
			elif x < -1:
				pix = (-(x+dx), -(y+dx))
			else:
				pix = (-x, y+dx)
		elif x > 1:
			pix = (x-dx, -y)
		else:
			pix = (x+dx, -y)
	elif fabs(ax - ay) > 0.5:
		if ax - 0.5 > ay:
			# left and right triangles
			if   x > 0 and y > 0:
				pix = (0.5 + y,   0.5 - x)
			elif x > 0 and y < 0:
				pix = (0.5 - y,  -0.5 + x)
			elif x < 0 and y > 0:
				pix = (-0.5 - y,  0.5 + x)
			elif x < 0 and y < 0:
				pix = (-0.5 + y, -0.5 - x)
		else:
			# top and bottom triangles
			if   y > 0 and x > 0:
				pix = (0.5 - y,  0.5 + x)
			elif y > 0 and x < 0:
				pix = (-0.5 + y, 0.5 - x)
			elif y < 0 and x > 0:
				pix = (0.5 + y, -0.5 - x)
			elif y < 0 and x < 0:
				pix = (-0.5 - y, -0.5 + x)
	else:
		# OK.
		pix = (x, y)

	# Now check if this is a "halfpixel" and add its
	# second piece
	if fabs(fabs(pix[0]) - fabs(pix[1])) == 0.5:
		(x, y) = pix
		if fabs(x) - 0.5 == fabs(y):
			pix2 = (x, -y)
		else:
			pix2 = (-x, y)
		ret = (pix, pix2)
	else:
		ret = (pix,)

	for (cx, cy) in ret:
		if fabs(fabs(cx) - fabs(cy)) > 0.5 or fabs(cx) >= 1 or fabs(cy) >= 1:
			raise Exception("map_to_valid_pixels: ", x, y, " mapped to illegal value: ", cx, cy)

	return ret

def neighbors(x, y, level, include_self=False):
	# Return a set of immediate neighbors of the pixel
	# into which (x,y) falls
	result = set()
	dx = pix_size(level)
	(cx, cy) = xy_center(x, y, level)
	#(cx, cy, dx) = (x, y, level)
	for (cx, cy) in map_to_valid_pixels(cx, cy, dx):
#		print (cx, cy)
		for di in xrange(-1, 2):
			for dj in xrange(-1, 2):
				if not include_self and di == 0 and dj == 0:
					continue

				(x, y) = (cx + di*dx, cy + dj*dx)
				pix = map_to_valid_pixels(x, y, dx)
#				print '  n=', di, dj, pix
				result.update(pix)

	return result
################################################################


#testview()
#from matplotlib.pyplot import *
#import random as ran
#testpix()

if __name__ == '__main__':
	clon = 212; clat = 12;
	lon1 = 212.2; lat1 = 15.1;
	lon2 = 212; lat2 = 15.11;
	(x1, y1) = gnomonic(lon1, lat1, clon, clat)
	(x2, y2) = gnomonic(lon2, lat2, clon, clat)
	dx = x2 - x1
	dy = y2 - y1
	d = (dx**2 + dy**2)**.5
	d0 = gc_dist(lon1, lat1, lon2, lat2)
	print d, d0, d/d0 - 1
	#print x, y, (x*x + y*y)**.5, gc_dist(lon1)
	exit()

if __name__ == '__main__':
	from numpy.random import random
	l = random(100000)*360
	b = random(100000)*180 - 90
	print len(l), ' items'
	x, y = proj_bhealpix(l, b)
	l1, b1 = deproj_bhealpix(x, y)
	print np.allclose(l, l1, 1e-10, 1e-10), np.allclose(b, b1, 1e-10, 1e-10)
	exit()

