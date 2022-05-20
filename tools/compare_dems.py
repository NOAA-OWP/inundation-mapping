#!/usr/bin/env python3

import rasterio 
import numpy as np
import argparse
import os
from glob import glob


def get_dem_filenames(resolution,dem_source):

    glob(f'/data/inputs/{dem_source}/dem_3dep_*_{resolution}.vrt'
