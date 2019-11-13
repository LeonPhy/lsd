try:
    import astropy.io.fits as pyfits
except ImportError:
    import pyfits

import argparse, os, keyword, glob
import numpy as np
from lsd import DB

table_def = ({
    'filters' : { 'complevel': 5, 'complib': 'blosc', 'fletcher32': False },
    'schema' : {
           'main' : {
                   'columns': [ ('_id', 'u8') ],
                   'primary_key' : '_id',
                   'spatial_keys': ('ra', 'dec'),
                   },
           },
})

def fix_names(dtype, ra, dec):
    names = [n.lower() for n in dtype.names]
    if ra is not '':
        names[names.index(ra)] = 'ra'
    if dec is not '':
        names[names.index(dec)] = 'dec'
    for i, n in enumerate(names):
        if keyword.iskeyword(n):
            names[i] = n+'_'
    dtype.names = names

def import_file(file, table, ra, dec, is_radians):
    try:
        dat = np.array(pyfits.getdata(file, 1)[:])
    except IndexError:
        dat = 0
        yield (file, 0)
    except Exception as e:
        print 'Could not read file {:s}'.format(file)
        raise e

    if dat != 0:
        # FITS capitalization problems...
        dtype = dat.dtype
        fix_names(dtype, ra, dec)
        #pdb.set_trace()

        if is_radians:
            dat['ra'][:] = np.degrees(dat['ra'][:])
            dat['dec'][:] = np.degrees(dat['dec'][:])

        ids = table.append(dat)
        yield (file, len(ids))

def import_fits(db, table, filenames, ra='', dec='', is_radians=False):
	from lsd import pool2
	import re

    # Figure out the schema from the first file in the list
    try:
        firstfile = pyfits.getdata(filenames[0], 1)
    except Exception as e:
        print 'Could not read first file: {}'.format(filenames[0])
        raise e

    dtype = firstfile.dtype
    fix_names(dtype, ra, dec)
    columns = table_def['schema']['main']['columns']
    for typedesc in dtype.descr:
        name = typedesc[0]
        type = typedesc[1]
        if (type[0] == '<') or (type[0] == '>'):
            type = type[1:]
        if len(typedesc) > 2:
            type = '{}'.format(typedesc[2][0]) + type
            #tdescr += (typedesc[2],)
            # bit of a hack, but doesn't work with the more complicated format
            # specification and FITS binary tables don't support multidimensional
            # arrays as columns.
        tdescr = (name, type)
        columns.append(tdescr)

    pool = pool2.Pool()
    db = DB(db)
    with db.transaction():
        if not db.table_exists(table):
            table = db.create_table(table, table_def)
        else:
            table = db.table(table)
        for fn, num in pool.imap_unordered(filenames, import_file,
                                           (table, ra, dec, is_radians)):
            print 'Imported file {:s} containing {:d} entries.'.format(fn, num)


def main():
    parser = argparse.ArgumentParser(description='Import FITS files to LSD')
    parser.add_argument('--db', '-d', default=os.environ['LSD_DB'])
    parser.add_argument('--ra', default='ra', help='Column in FITS file to rename "ra"')
    parser.add_argument('--dec', default='dec', help='Column in FITS file to rename "dec"')
    parser.add_argument('--radians', '-rad', action='store_true', help='RA/Dec are in radians.')
    parser.add_argument('table', type=str, help='Name of table to create')
    parser.add_argument('filenames', type=str, nargs='+', help='FITS files to import. Can use globbing wildcards.')
    args = parser.parse_args()

    # Construct a list of files to load
    filenames = []
    for fn_pattern in args.filenames:
        filenames += glob.glob(fn_pattern)

    print '{:d} files supplied.'.format(len(filenames))
    print filenames

    import_fits(args.db, args.table, filenames,
                ra=args.ra, dec=args.dec,
                is_radians=args.radians)


if __name__ == "__main__":
    main()
