# ==============================================================================
# SYSTEM PERFORMANCE, TUNING AND PARELLIZATION OPTIMIZATIONS.
# KEY WORDS: optimization and control to help manage mostly memory overloads.

# ----------------------------------------------
# Oct 2026: It is expected that some of these values will change as we figuring out
# the right optimizations per environment run.
# Later, we may even consider having two optimization files, one for standard
# development for any NOAA or non NOAA user, and a second for production (thor)
# usage. If we do eventually create seperate one for dev versus prod, we will need
# seperate docker images.  ie. DockerFile.dev and Dockerfile.prod (or similar)
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
#  This has been optimized for dev systems (128 GiB RAM & 16 Cores), but will help prod enviros significantly.


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
# Normally, you will likely not override it depending if you are using Multi-Proc, Multi-Thread
# bash parallel or not.



# ==============================================================================
# GeoTIFF & Raster Caching & VSI RAM Buffers
# (2 GB block cache + 1 GB VSI read buffer per worker)
#    GDAL_CACHEMAX (The Processing Desk):
#    This is the active RAM buffer where GDAL holds raw raster pixels, decompressed image tiles,
#    and array blocks currently being read or modified by your Python code.
#    Defaults to only 5% of RAM (per process)
# 
#  128 MiB per rank (gdal / mpiexec num of workers (not fim -jh or -jb))
# Might seem low, but it is only for min cache reserved. If it needs more, it will use it
# and it will not degrade performance. But the smaller values means when mulitple -jh and -jb are running
# it is not holding as much default min cache.
# Note: in run_taudem_subprocess which is volite especially in the d8flowdir, that code
# dynamically adjusts for that processing only and adjusts based on dem file size.
export GDAL_CACHEMAX=128  # Optimized for files < 100 MiB

# Note:
# Since you are mostly processing single standalone GeoTIFFs, you do not need GDAL_MAX_DATASET_POOL_SIZE
# or GDAL_MAX_DATASET_POOL_RAM_USAGE at all. Both settings only manage Virtual Raster (.vrt) proxy pools
# when GDAL opens dozens or hundreds of source tiles at once.

# ==============================================================================
# --- FILE I/O OPTIMIZATION FOR NETWORK MOUNTS (EFS/NFS) (and Parallel Worker Mounts) ---
# This helps with network shared EFS calls.
# CHANGED: 'YES' completely blocks chatty filesystem directory scans across network mounts
# Skip Slow Directory Scans
export GDAL_DISABLE_READDIR_ON_OPEN=YES
export OGR_DISABLE_READDIR_ON_OPEN=YES

# =====================================================================
# --- THREAD LOCKDOWN & PROCESS POOL CONCURRENCY GOVERNOR ---
# Crucial for Geoparquet/PyArrow and Python ProcessPool
# Prevents child math/transform libraries from spawning threads that thrash your 30 workers
# Prevent hidden thread explosions across OpenMP, BLAS, GDAL, and Apache Arrow (Parquet)
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export GDAL_NUM_THREADS=1

# ------------------------
# OpenMPI 4.1.6 Docker & Unbinding Settings
# Dynamically ensure runtime MPI bypass blocks are always globally active
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
export OMPI_MCA_hwloc_base_binding_policy=none
# Fixes a common OpenMPI warning/crash regarding shared memory access in Docker
export OMPI_MCA_btl_vader_single_copy_mechanism=none
# Restricts OpenMPI to localhost loopback inside the container (prevents looking for external cluster fabrics)
export OMPI_MCA_btl=self,vader,tcp
export OMPI_MCA_btl_tcp_if_include=lo


# ==============================================================================
# --- VECTOR PERFORMANCE (Optimized for 2-20 MiB GeoPackages and Parquet files) ---
# This guarantees that the entire GeoPackage database index and data pages fit entirely in RAM cache.
# sets a 1,024 MB / 1 GB SQLite cache per worker).
export OGR_SQLITE_CACHE=1024
export OGR_SQLITE_SYNCHRONOUS=OFF

# =====================================================================
# --- PYARROW & GEOPARQUET SYSTEM ENHANCEMENTS ---

# -----------------
# Optimize GDAL/OGR's built-in GeoParquet driver thread pool 
# export OGR_PARQUET_NUM_THREADS=1

# Advanced GeoParquet Optimization
# export OGR_PARQUET_ROW_GROUP_SIZE=262144  -- Nov 2026: Intentionally commented out, testing required
# export OGR_PARQUET_COMPRESSION=ZSTD  -- Nov 2026: Intentionally commented out, testing required

# Restricts PyArrow to 1 core per process to align with your ProcessPool layout
# also helps Parquet
export ARROW_NUM_THREADS=1
export ARROW_IO_THREADS=1
# Prevent Memory Fragmentation and Bloat
# Forces PyArrow's underlying C++ allocator to aggressively release memory 
# back to the operating system after reading a row group, rather than caching it.
export ARROW_DEFAULT_MEMORY_POOL=jemalloc

# (from Gemini about ARROR_DEFAULT_MEMORY_POOL)
# Why it helps: Recent versions of PyArrow (v18+) switched their default memory allocator to mimalloc, 
# which introduced a known memory growth/leak bug when reading many Parquet files in a loop.
# Setting this back to jemalloc prevents RSS memory bloat and reuses allocated memory blocks quickly without
# making constant system calls to Linux.

# Important caveat: By design, jemalloc holds onto freed memory blocks for a short time so it can reuse them quickly.
# Across 12 workers, that is totally fine on your 128 GiB RAM machine, though if you ever see RAM climbing higher
#  than you want, you can also set export MALLOC_CONF="dirty_decay_ms:1000,muzzy_decay_ms:1000" 
# to make jemalloc return unused RAM to the OS every second

# export MALLOC_CONF="narenas:2,dirty_decay_ms:1000,muzzy_decay_ms:5000"
export MALLOC_CONF="narenas:2,dirty_decay_ms:1000,muzzy_decay_ms:1000,background_thread:true"
# jemalloc (MALLOC_CONF): jemalloc to keep freed memory warm for instant reuse rather than constantly handing
# it back to Linux, while also limiting unnecessary internal arenas since your workers are single-threaded:

# =====================================================================
# --- LOW-LEVEL LINUX & GLIBC MEMORY ALLOCATION TUNING ---

# Prevent Virtual Memory Arena Proliferation (Crucial for Multi-Processing)
# By default, glibc creates up to 8 memory arenas per core (64 cores = 512 arenas!).
# This forces glibc to limit memory pools to 2 per process, stopping memory leaks.
export MALLOC_ARENA_MAX=2

# Impacts screen output for terminals
export DISPLAY=":0"

