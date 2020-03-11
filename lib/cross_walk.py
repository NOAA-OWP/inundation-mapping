#!/usr/bin/env python3

import numpy as np
from numba import njit, typeof, typed, types
import argparse
from raster import Raster


derived_catchments = Raster('../data/test2/outputs/demDerived_streamPixels.tif')
nwm_catchments = Raster('../data/nwm/')
