"""
HAND Inundation from Heights 
"""

import pandas as pd
import numpy as np
from raster import raster
from gdal import GDT_UInt16


class handInundation(raster):

	@classmethod
	def handInundationFromHeightsTable(self,handRaster_fileName,catchmaskRaster_fileName,heightsTable_fileName,mapping={'inundated':2,'non-inundated':1,'ndv':0},inundationMap_dtype=np.uint8,inundationMap_fileName=None,verbose=False,convertFeetToMeters=False):

		"""
		Writes out hand binary inundation raster given heights by COMID

		usage: 
			from handInundation import handInundation
			
			hand_inundation_map = handInundation.handInundationFromHeightsTable(handRaster_fileName,catchmaskRaster_fileName,heightsTable_fileName,mapping={'inundated':2,'non-inundated':1,'ndv':0},inundationMap_dtype=np.uint8,inundationMap_fileName=None,verbose=False,convertFeetToMeters=False)

		requirements:
			python3
			python3 modules: pandas, numpy, gdal, copy, os
			raster sub-module
		"""


		if verbose: print("Generating HAND Inundation Map ...")

		# load data
		handRaster = raster(handRaster_fileName)
		catchmaskRaster = raster(catchmaskRaster_fileName)
		heightsTable = pd.read_csv(heightsTable_fileName,names=['heights'],index_col=0,header=0,dtype={'heights':np.float64})

		# initialize handInundation Raster
		self.handInundationRaster = handRaster.copy()
		self.handInundationRaster.array = np.full(handRaster.dim,mapping['ndv'],dtype=inundationMap_dtype)
		self.handInundationRaster.ndv = mapping['ndv']

		# conversion of feet to meters if necessary
		if convertFeetToMeters:
			heightsTable['heights'] = heightsTable['heights'] / 3.28084

		for comid,height in heightsTable.iterrows():
			
			if verbose: print("COMID: {}".format(comid))

			indicesOfCatchmask = np.where(catchmaskRaster.array==comid)
			handValues = handRaster.array[indicesOfCatchmask]

			inundatedValues = np.full(handValues.shape,mapping['ndv'],dtype=inundationMap_dtype)
			inundatedValues[handValues <= height['heights']] = mapping['inundated']
			inundatedValues[handValues > height['heights']] = mapping['non-inundated']

			self.handInundationRaster.array[indicesOfCatchmask] = inundatedValues
		
		
		# write out map
		if inundationMap_fileName is not None:
			
			def writeRaster(self,**kwargs):
				super().writeRaster(**kwargs)

			self.handInundationRaster.writeRaster(fileName=inundationMap_fileName,dtype=GDT_UInt16,verbose=verbose)

		return(self)


	def copy(self):
		super().__init__.copy()
	
	def writeRaster(self,**kwargs):
		super().writeRaster(**kwargs)

if __name__ == '__main__':

	handRaster_fileName = '/home/lqc/Documents/research/fist/quickComparsion/020501hand_clipped.tif'
	catchmaskRaster_fileName = '/home/lqc/Documents/research/fist/quickComparsion/020501catchmask_clipped.tif'
	
	heightsTable_fileName = '/home/lqc/Documents/research/fist/quickComparsion/COMID_sample_data_grouped_HEC.csv'
	#heightsTable_fileName = '/home/lqc/Documents/research/fist/quickComparison/COMID_sample_data_grouped_linear.csv'

	verbose = True
	mapping = {'inundated':2,'non-inundated':1,'ndv':0}
	inundationMap_fileName = '/home/lqc/Documents/research/fist/quickComparsion/handInundation2.tiff'
	convertFeetToMeters = True

	handInundation.handInundationFromHeightsTable(handRaster_fileName,catchmaskRaster_fileName,heightsTable_fileName,
												mapping=mapping,inundationMap_dtype=np.uint8,
												inundationMap_fileName=inundationMap_fileName,verbose=verbose,convertFeetToMeters=convertFeetToMeters)