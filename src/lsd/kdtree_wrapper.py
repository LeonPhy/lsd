"""
Module written to replace deprecated scikits.ann package, will import scipy.spatial cKDTree 
to do the same job with ~5% time increase since custom scikits.ann package not available
anymore. 

Author: Eddie Schlafly
"""

import numpy as np

try:
    from scikits.ann import kdtree

    def query(tree, xy1, num_nn):
        return tree.knn(xy1, num_nn)

except ImportError:
    from scipy.spatial import cKDTree as kdtree

    def query(tree, xy1, num_nn):
        match_d2, match_idxs = tree.query(xy1, k=num_nn)
        match_d2 = match_d2**2.
        if num_nn == 1:
            match_d2 = match_d2[..., np.newaxis]
            match_idxs = match_idxs[..., np.newaxis]
        return (match_idxs, match_d2)