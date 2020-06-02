#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from osgeo import gdal, ogr, osr
import numpy as np
from os.path import isfile
from os import remove
from copy import deepcopy
from subprocess import call

class Raster:

	"""
	Raster object from single band rasters

	...

	Attributes
	----------
	array : numpy array
		raster data in numpy array
	gt : list
		geotransform. see gdal docs for more info.
	proj : str
		Projection string
	ndv : number
		No data value
	des : str
		band description
	ct : gdal.colorTable
		color table
	dt : int
		GDAL GDT data type. See notes.
	dim : tuple
		raster dimensions (bands, rows, columns) for multi-bands and (row, columns) for single band
	nbands : int
		number of bands.
	nrows : int
		number of rows
	ncols : int
		number of columns

	Methods
	-------
	writeRaster(fileName,dtype=None,driverName='GTiff',verbose=False)
		Write out raster file as geotiff
	copy()
		Copy method. Uses deepcopy since array data is present
	clipToVector(raster_fileName,vector_fileName,verbose=False,output_fileType='GTiff',output_fileName=None,loadOutput=True)
		Clips to vector using gdalwarp command line utility

	Raises
	------
	OSError
		If fileName does not exist
	ValueError
		Raises if input raster

	See Also
	--------

	Notes
	-----
	Currently only accepts single band rasters.

	Multiple datatypes are used. The table below shows which numpy datatypes correspond to the the GDAL types and their integer codes.

	#  ## Integer Code ##   ## Global Descriptor Table ##      ## Numpy ##
  	#         0                     GDT_Unknown                   NA
  	#         1  				    GDT_Byte                      np.bool, np.int ,np.int8, np.long, np.byte, np.uint8
  	#         2      				GDT_UInt16                    np.uint16, np.ushort
  	#         3     				GDT_Int16                     np.int16, np.short
  	#         4       				GDT_UInt32                    np.uint32 , np.uintc
  	#         5       				GDT_Int32                     np.int32, np.intc
  	#         6       				GDT_Float32                   np.float32, np.single
  	#         7       				GDT_Float64                   np.float64, np.double
  	#         8       				GDT_CInt16                    np.complex64
  	#         9       				GDT_CInt32                    np.complex64
  	#         10       				GDT_CFloat32                  np.complex64
  	#         11       				GDT_CFloat64                  np.complex128
  	#         12       				GDT_TypeCount                 NA

	Examples
	--------
	Load Raster
	>>> rasterData = fldpln.Raster('path/to/raster')

	"""

	# converts numpy datatypes and gdal GDT variables to integer codes
	dataTypeConversion_name_to_integer = { np.int8 : 1 , np.bool : 1 , np.int : 1 , np.long : 1 , np.byte : 1, np.uint8 : 1,
										   np.uint16 : 2 , np.int16 : 3 ,
										   np.ushort : 2 , np.short : 3 ,
										   np.uint32 : 4 , np.uintc : 4 , np.int32 : 5 , np.intc : 5 ,
										   np.float32 : 6 , np.single : 6 ,
										   np.float64 : 7 , np.double : 7 ,
										   np.complex64 : 10 , np.complex128 : 11 ,
										   0:0,1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8,9:9,10:10,11:11,12:12 }

	# converts integer codes and gdal GDT variables to numpy datatypes
	dataTypeConversion_integer_to_name = {0 : np.complex128 , 1 : np.int8 , 2 : np.uint16 , 3 : np.int16 ,
										  4 : np.uint32 , 5 : np.int32 , 6 : np.float32 , 7 : np.float64 ,
										  8 : np.complex64 , 9 : np.complex64 , 10 : np.complex64 , 11 : np.complex128 }


	def __init__(self,fileName,loadArray=True,dtype=None):

		"""
		Initializes Raster Instance from single band raster

		...

		Parameters
		----------
		fileName : str
			File path to single band raster
		dtype : numpy datatype or int, optional
			Numpy, GDT, or integer code data type used to override the data type on the file when imported to array (Default Value = None, None sets to the numpy array data type to the one in the raster file)

		Returns
		-------
		raster
			Instance of raster object

		"""

		if not isfile(fileName):
			raise OSError("File \'{}\' does not exist".format(fileName))

		stream = gdal.Open(fileName,gdal.GA_ReadOnly)

		self.nrows,self.ncols = stream.RasterYSize , stream.RasterXSize
		self.nbands = stream.RasterCount

		if loadArray:
			self.array = stream.ReadAsArray()

		self.gt = stream.GetGeoTransform()
		self.proj = stream.GetProjection()

		# if self.nbands > 1:
			# raise ValueError('Raster class only accepts single band rasters for now')

		band = stream.GetRasterBand(1)

		self.ndv = band.GetNoDataValue()

		# set data type
		if dtype is not None: # override raster file type

			# sets dt to dtype integer code
			try:
				self.dt = self.dataTypeConversion_name_to_integer[dtype]
			except KeyError:
				raise ValueError('{} dtype parameter not accepted. check docs for valid input or set to None to use data type from raster'.format(dtype))

			# sets array data type
			if isinstance(dtype,type): # if dtype is a numpy data tpe

				self.array = self.array.astype(dtype)

			else: # if dtype is an integer code of GDAL GDT variable

				try:
					self.array = self.array.astype(self.dataTypeConversion_integer_to_name[dtype])
				except KeyError:
					raise ValueError('{} dtype parameter not accepted. check docs for valid input or set to None to use data type from raster'.format(dtype))

		else: # sets to default data type in raster file

			self.dt = band.DataType

			try:
				self.array.astype(self.dataTypeConversion_integer_to_name[self.dt])
			except KeyError:
				raise ValueError('{} dtype parameter not accepted. check docs for valid input or set to None to use data type from raster'.format(self.dt))

		try:
			self.des = band.GetDescription()
		except AttributeError:
			pass

		try:
			self.ct = stream.GetRasterColorTable()
		except AttributeError:
			pass

		# self.dim = self.array.shape
		self.fileName = fileName

		stream,band = None,None


	@property
	def dim(self):
		""" Property method for number of dimensions """

		if self.nbands == 1:
			DIMS = self.nrows,self.ncols
		if self.nbands > 1:
			DIMS = self.nbands,self.nrows,self.ncols

		return(DIMS)


	def copy(self):
		""" Copy method. Uses deepcopy since array data is present """
		return(deepcopy(self))


	def writeRaster(self,fileName,dtype=None,driverName='GTiff',verbose=False):

		"""
		Write out raster file as geotiff

		Parameters
		----------
		fileName : str
			File path to output raster to
		dtype : numpy datatype or int, optional
			Numpy, GDT, or integer code data type (Default Value = self.dt attribute value, otherwise uses data type from the numpy array)
		driverName : str, optional
			GDAL driver type. See gdal docs for more details. Only tested for GTiff. (Default Value = 'GTiff')
		verbose : Boolean, optional
			Verbose output (Default Value = False)

		Returns
		-------
		None

		Raises
		------
		ValueError
			Raises ValueError when the data type parameter is not recognized. See the help docs for raster class to see which numpy, gdal, or encoded values are accepted.

		Examples
		--------
		Write Geotiff raster
		>>> rasterData = fldpln.Raster('path/to/raster')
		>>> rasterData.writeRaster('/different/path/to/raster',dtype=np.int8)

		"""

		driver = gdal.GetDriverByName(driverName)

		if dtype is None:
			try:
				dtype = self.dt
			except AttributeError:
				# dtype = gdal.GDT_Float64
				try:
					dtype = self.dataTypeConversion_name_to_integer[self.array.dtype]
				except KeyError:
					raise ValueError('{} dtype parameter not accepted. check docs for valid input or set to None to use data type from numpy array'.format(self.array.dtype))
		else:
			try:
				dtype = self.dataTypeConversion_name_to_integer[dtype]
			except KeyError:
				raise ValueError('{} dtype parameter not accepted. check docs for valid input or set to None to use data type from numpy array'.format(self.array.dtype))

		dataset = driver.Create(fileName, self.ncols, self.nrows, 1, dtype)
		dataset.SetGeoTransform(self.gt)
		dataset.SetProjection(self.proj)
		band = dataset.GetRasterBand(1)

		# set color table and color interpretation
		#print(band.__dict__)
		try:
			band.SetRasterColorTable(self.ct)
			#band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
		except AttributeError:
			pass

		try:
			band.SetDescription(self.des)
		except AttributeError:
			pass

		band.SetNoDataValue(self.ndv)
		band.WriteArray(self.array)
		band, dataset = None,None  # Close the file

		if verbose:
			print("Successfully wrote out raster to {}".format(fileName))

	def polygonize(self,vector_fileName,vector_driver,layer_name,verbose):

		gdal.UseExceptions()

		#  get raster datasource
		#
		src_ds = gdal.Open( self.fileName )
		srcband = src_ds.GetRasterBand(1)

		#
		#  create output datasource
		driver_ext_dict = {'ESRI Shapefile' : 'shp' , 'GPKG' : 'gpkg'}

		if vector_driver not in driver_ext_dict:
			raise ValueError('Driver not found in {}'.format(driver_ext_dict))

		drv = ogr.GetDriverByName(vector_driver)
		dst_ds = drv.CreateDataSource( vector_fileName)

		srs = osr.SpatialReference()
		srs.ImportFromWkt(self.proj)

		dst_layer = dst_ds.CreateLayer(layer_name, srs = srs, geom_type = ogr.wkbPolygon )

		if verbose:
			prog_func = gdal.TermProgress_nocb
		else:
			prog_func = None

		gdal.Polygonize( srcband, None, dst_layer, -1, ['8CONNECTED=8'], callback=prog_func )

	@classmethod
	def clipToVector(cls,raster_fileName,vector_fileName,output_fileName=None,output_fileType='GTiff',verbose=False):
		"""
		Clips to vector using gdalwarp command line utility

		...

		Parameters
		----------
		raster_fileName : str
			File path to raster to clip
		vector_fileName : str
			File path to vector layer to clip with
		output_fileName : str
			Set file path to output clipped raster (Default Value = None)
		output_fileType : str
			Set file type of output from GDAL drivers list (Default Value = 'GTiff')
		verbose : Boolean
			Verbose output (Default Value = False)

		Returns
		-------
		raster : raster
			Clipped raster layer

		Notes
		-----
		gdalwarp utility must be installed and callable via a subprocess

		Examples
		--------
		clip raster and don't return
		>>> fldpln.raster.clipToVector('path/to/raster','path/to/clipping/vector','path/to/write/output/raster/to')
		Clip raster and return but don't write
		>>> clippedRaster = fldpln.raster.clipToVector('path/to/raster','path/to/clipping/vector')


		"""

		# create temp output if none is desired
		if output_fileName is None:
			output_fileName = 'temp.tif'

		# generate command
		command = ['gdalwarp','-overwrite','-of',output_fileType,'-cutline',vector_fileName,'-crop_to_cutline',raster_fileName,output_fileName]

		# insert quiet flag if not verbose
		if not verbose:
			command = command.insert(1,'-q')

		# call command
		call(command)

		# remove temp file
		if output_fileName is None:
			remove(output_fileName)

		return(cls(output_fileName))

	def getCoordinatesFromIndex(self,row,col):
		"""
		Returns coordinates in the rasters projection from a given multi-index

		"""

		# extract variables for readability
		x_upper_limit, y_upper_limit = self.gt[0], self.gt[3]
		x_resolution, y_resolution = self.gt[1], self.gt[5]
		nrows, ncols = self.nrows, self.ncols

		x = x_upper_limit + (col * x_resolution)
		y = y_upper_limit + (row * y_resolution)

		return(x,y)


	def sampleFromCoordinates(self,x,y,returns='value'):
		"""
		Sample raster value from coordinates
		...

		Parameters
		----------
		raster_fileName : str
			File path to raster to clip
		vector_fileName : str
			File path to vector layer to clip with
		output_fileName : str
			Set file path to output clipped raster (Default Value = None)
		output_fileType : str
			Set file type of output from GDAL drivers list (Default Value = 'GTiff')
		verbose : Boolean
			Verbose output (Default Value = False)

		Returns
		-------
		raster : raster
			Clipped raster layer

		Notes
		-----
		gdalwarp utility must be installed and callable via a subprocess

		Examples
		--------
		clip raster and don't return
		>>> fldpln.raster.clipToVector('path/to/raster','path/to/clipping/vector','path/to/write/output/raster/to')
		Clip raster and return but don't write
		>>> clippedRaster = fldpln.raster.clipToVector('path/to/raster','path/to/clipping/vector')


		"""

		# extract variables for readability
		x_upper_limit, y_upper_limit = self.gt[0], self.gt[3]
		x_resolution, y_resolution = self.gt[1], self.gt[5]
		nrows, ncols = self.nrows, self.ncols

		# get upper left hand corner coordinates from the centroid coordinates of the upper left pixel
		x_upper_limit =  x_upper_limit - (x_resolution/2)
		y_upper_limit = y_upper_limit - (y_resolution/2)

		# get indices
		columnIndex = int( ( x - x_upper_limit) / x_resolution)
		rowIndex = int( ( y - y_upper_limit) / y_resolution)

		# check indices lie within raster limits
		columnIndexInRange = ncols > columnIndex >= 0
		rowIndexInRange = nrows > rowIndex >= 0

		if (not columnIndexInRange) | (not rowIndexInRange):
			raise ValueError("Row Index {} or column index {} not in raster range ({},{})".format(rowIndex,columnIndex,nrows,ncols))

		# check value is not ndv
		if self.array[rowIndex,columnIndex] == self.ndv:
			raise ValueError("Sample value is no data at ({},{})".format(nrows,ncols))

		# return if statements
		if returns == 'value':
			return(self.array[rowIndex,columnIndex])
		elif returns == 'multi-index':
			return(rowIndex,columnIndex)
		elif returns == 'ravel-index':
			return(np.ravel_multi_index((rowIndex,columnIndex),(nrows,ncols)))
		else:
			raise ValueError('Enter valid returns argument')
