
module purge
module load mpich gdal2-stack
#module load GCC/5.1.0-binutils-2.25
module load CMake GCC/4.9.2-binutils-2.25
echo "GDAL_HOME=$GDAL_HOME"
bdir=`dirname $0`/build
[ -d $bdir ] && echo "Deleting $bdir ..." && rm -fr $bdir
mkdir build
cd build
cmake .. -DGDAL_INCLUDE_DIR=${GDAL_HOME}/include
make
