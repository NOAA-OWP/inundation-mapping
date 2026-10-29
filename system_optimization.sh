# ==============================================================================
# SYSTEM PERFORMANCE, TUNING AND PARELLIZATION OPTIMIZATIONS.
# KEY WORDS: optimization and control to help manage mostly memory overloads.

# ----------------------------------------------
# Oct 2026: It is expected that some of these values will change as we figuring out
# the right optimizations per environment run.
# Later, we may even consider having two optimization files, one for standard
# development for any NOAA or non NOAA user, and a second for production
# usage. 
# ----------------------------------------------

# NOTE: This has to be an .sh file and is called by entrypoint.sh for DockerBuilds
# so it is baked into the environment. But.. you can change it and test it by simply
# update the .sh file and using a typical bash run command. ie) bash system_optimizations.sh

# ----------------------------------------------
# These are various system performance, tuning and parallelization management
# This includes optimizations:
#   - GDAL, GDAL engine, C++ engine / usage, OGR
#   - Various vector and raster files
#   - Network and file i/o mostly for network mounts such as EFS or external I/O mounts
#   - rasterio, geopandas, parquet, pyarrow, 
#   - Memory and CPU management and over throttling
# 
#  This has been optimized for larger system (128 GiB - 256 GiB RAM and 16 to 64 Cores)
#  but it optimization only.

# ==============================================================================
# GDAL PERFORMANCE TUNING
# Advanced GDAL/OGR Driver Optimizations and Mosaic Optimizations

# *****  NOTES about GDAL Overrides  *****

# When using gdalwarp, this can be overridden using the -co NUM_THREADS=ALL_CPUS
# or similar, adjusting for the number of threads, default will set to 1

# If you are using gdal via rasterio, you can optimize by using a rasterio enviro.
# import rasterio
# from rasterio.warp import calculate_default_transform, reproject, Resampling

# # Temporarily open up all CPU cores specifically for this heavy warp block
# adjusting the NUM THREADS to a value that makes sense for parallelization
# But you only need the outside rasterio.Env with if you want to override the two
# defaults which are 1.  Key: PER PROCESS (mp or -jh)
# ie)  with rasterio.Env(GDAL_NUM_THREADS='ALL_CPUS', NUM_THREADS='ALL_CPUS'):
# with rasterio.Env(GDAL_NUM_THREADS='4', NUM_THREADS='4'):
#     with rasterio.open('input.tif') as src:
#         # Perform your heavy reproject / warp here...
#         pass

# -----------
# GDAL_CACHEMAX, GDAL_MAX_DATASET_POOL_SIZE, GDAL_MAX_DATASET_POOL_RAM_USAGE
# 1. GDAL_CACHEMAX (The Processing Desk):
#    This is the active RAM buffer where GDAL holds raw raster pixels, decompressed image tiles,
#    and array blocks currently being read or modified by your Python code.
#    Defaults to only 5% of RAM (per process)

# Also prevents small network/disk calls from overwhelming the filesystem
# Per process !!! (4 GiB)
export GDAL_CACHEMAX=5124

# 2. GDAL_MAX_DATASET_POOL_SIZE (The File Handle Limit):
#    This sets a cap on the maximum number of structural file handles (the underlying files inside a VRT,
#    composite raster, or database connection) that GDAL can keep "open and indexed" simultaneously to 
#    avoid the time penalty of closing and re-opening them.

# 3. GDAL_MAX_DATASET_POOL_RAM_USAGE (The File Handle Vault Door):
#    This is a safety brake. It caps the amount of RAM the files inside your dataset pool can collectively
#    consume just to keep their metadata headers and compression tables cached in memory.

#############


# 2.  Enhanced settings for large multi-file mosaics, mostly when downloading
# data files via external vrt.
# Defaults to 100
# export GDAL_MAX_DATASET_POOL_SIZE=2000 

# 3. HARD MEMORY PROTECTION: Caps how much RAM the pooled datasets can consume.
# Prevents large underlying files from triggering an OOM crash.
# This is per process and helps balance various usage of GDAL tools
# vrts calls, and python packages with GDAL included.
# 512MB per process, and as we use -jh, this should be good.
# GDAL defaults to 25% of the total physical RAM available to the system.
export GDAL_MAX_DATASET_POOL_RAM_USAGE=512MB

####################
# The overall GDAL overview
# With GDAL_CACHEMAX=5124 (4 GiB) and GDAL_MAX_DATASET_POOL_RAM_USAGE=512MB (0.5 GiB),
# each worker process is explicitly restricted to a maximum of 6.0 GiB of C++ layer allocations.

# ==============================================================================
# --- FILE I/O OPTIMIZATION FOR NETWORK MOUNTS (EFS/NFS) (and Parallel Worker Mounts) ---
# This helps with network shared EFS calls.
# CHANGED: 'YES' completely blocks chatty filesystem directory scans across network mounts
export GDAL_DISABLE_READDIR_ON_OPEN=YES
export OGR_DISABLE_READDIR_ON_OPEN=YES


# ==============================================================================
# --- VECTOR PERFORMANCE (Optimized for 2-20 MiB GeoPackages and Parquet files) ---
# Allocate 32MB per process. This guarantees 
# that the entire GeoPackage database index and data pages fit entirely in RAM cache.
export OGR_SQLITE_CACHE=32
export OGR_SQLITE_SYNCHRONOUS=OFF

# =====================================================================
# Optimize GDAL/OGR's built-in GeoParquet driver thread pool 
# export OGR_PARQUET_NUM_THREADS=1

# Advanced GeoParquet Optimization
# export OGR_PARQUET_ROW_GROUP_SIZE=262144
# export OGR_PARQUET_COMPRESSION=ZSTD


# =====================================================================
# --- THREAD LOCKDOWN & PROCESS POOL CONCURRENCY GOVERNOR ---
# Crucial for Geoparquet/PyArrow and Python ProcessPool
# Prevents child math/transform libraries from spawning threads that thrash your 30 workers
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export GDAL_NUM_THREADS=1


# =====================================================================
# --- PYARROW & GEOPARQUET SYSTEM ENHANCEMENTS ---
# Restricts PyArrow to 1 core per process to align with your ProcessPool layout
export ARROW_NUM_THREADS=1
# Prevent Memory Fragmentation and Bloat
# Forces PyArrow's underlying C++ allocator to aggressively release memory 
# back to the operating system after reading a row group, rather than caching it.
export ARROW_DEFAULT_MEMORY_POOL=jemalloc
# Suppress Unused PyArrow Signals
# Prevents child worker processes from intercepting system interrupts,
# ensuring clean process terminations when your ProcessPool recycles workers.
export ARROW_ENABLE_THREADED_SIGNALS=0

export ARROW_SIMD_LEVEL=AVX2


# =====================================================================
# --- LOW-LEVEL LINUX & GLIBC MEMORY ALLOCATION TUNING ---

    # 1. Prevent Virtual Memory Arena Proliferation (Crucial for Multi-Processing)
# By default, glibc creates up to 8 memory arenas per core (64 cores = 512 arenas!).
# This forces glibc to limit memory pools to 2 per process, stopping memory leaks.
export MALLOC_ARENA_MAX=2

# 2. Force Aggressive Memory Return to OS
# Lowers the threshold (in bytes) at which glibc releases unmapped heap spaces 
# back to Linux. Keeps the container's resident set size (RSS) flat.
export MALLOC_TRIM_THRESHOLD_=131072

# 3. Disable Memory Allocation Padding
# Prevents glibc from leaving unused padding space around newly allocated 
# NumPy arrays or Arrow memory blocks.
export MALLOC_PERTURB_=0

# 4. Turn Off Speculative Memory Pre-allocation
# Stops the glibc allocator from aggressively hoarding contiguous blocks 
# of RAM when a worker process requests variable-sized raster windows.
export MMAP_THRESHOLD=131072

# Dynamically ensure runtime MPI bypass blocks are always globally active
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
