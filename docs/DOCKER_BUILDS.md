# Docker Builds

Our system uses a two part docker build package. One stage manually downloads TauDEM 5.5 code complies. The second stage copys the final compiled files from the TauDEM Builder to the second stage, the FIM docker stage.


----------------------------------------
## TauDEM Stage (Stage 1)

This stage is build using the osgeo/gdal:ubuntu-small-3.12.3, and complies TauDEM ulities, version 5.5.

While the TauDEM has a wide number of utilities available, we are complining on the packages we are using. After downloading source code, and compiling, completed file are put into a directory that can be passe3ed to stage 2.

Note:

When debugging, users can copy out the full stage 1 into a seperate Dockerfile, ie) `Dockerfile.taudem`, complile and test it independently. Then Stage 2 can simply reference the test Docker temporary TauDEM local docker image, which can be created using a standard docker buildx build command. When compiled as a test independent docker image, ensure the final CMD lines is uncommented, but can not be included when it is part of the full Dockerfile.dev.
    `CMD ["/bin/bash"]`

To reference it, comment out the entire Stage 1 from the main Dockerfile, uncomment the debug TauDEM stage code adjusting for the Docker image name, ie) `taudem_debug_image`, given here.


## FIM Stage (Stage 2)

This stage was build against osgeo/gdal:ubuntu-small-3.12.3, same as Stage 1 and it is critical all components are built against gdal 3.12.3 (Python 3.12).

## GDAL / numpy / fiona / rasterio / pyogrio

GDAL 3.12.3 does come with a number of Python 3.12 package but uses lower versions of package, specifically a low version of Numpy (numpy 1.26). With numpy being such a lower version, it forces most package to be very old versions.

By setting GDAL in specific patterns and folders, we can manually force it to use newer version of numpy which are fully compatiable. This allows many packages to be far newer versions.  Using `constraints.txt` file with `uv pip`, it forces all package to use GDAL 3.12.3 and allows numpy to be at least version 2.x.

Key other packages such as pyogrio, rasterio and numpy are also forced installed and updated in system folders to ensure specific GDAL 3.12.3. This is why some packages are not specifically included in the
requirments-min.ini file. If you need new versiosn of numpy, rasterio, fiona or pyogrio, manage those
in the dockerfile.

----------------------------------------
## uv pip
This system uses a tool named `uv pip`, which is very quick and efficient, far better than pipenv and standard pip, and very strong in ensuring critical versioning and dependencies.

Our Dockerfile will automatically enforce versioning of GDAL and numpy>2 via the constraints file, which have a critical interdependency. 

CAUTION: An upgrade of some packages could upgrade or downgrade multiple packages based on the interdependies of package. It is ok, if some packages get downgraded, but keep a close eye on versions of key package like geopandas, gval, numba, numpy, pandas, pyarrow, rasterio and xarray.


----------------------------------------
## Adding, Updating or Removing Packages
1. `Ensure you are already inside a container`
2. `requirements-min.ini`:
    - If you are `adding or removing a package`, start by updating the `foss_fim/requirements-min.ini` file. If you are just updating a package, skip this step.
    - Notice, requirements-min.ini does not have versions and does not necessarily have all packages. It does have all key packages. Packages on the list that you are ok with being brought in automatically as a dependency AND you not worried about its version, can be removed from the `requirements-min.ini` file.
3.  `Recompile`: 
    - If you are recompiling for an addition or deletion, your command becomes:
        ```
        uv pip compile /foss_fim/requirements-min.ini \
        --no-binary fiona \
        --no-binary rasterio \
        -o /foss_fim/requirements.txt
        ```
        IMPORTANT: This will upgrade all packages on the list for newer versions if applicable and if compatiable.
    - If you are updating a package(s), the command it identical for the one above with one extra line, which is the `upgrade-package` command.
        ```
        uv pip compile /foss_fim/requirements-min.ini \
        --upgrade-package <package-name>
        --no-binary fiona \
        --no-binary rasterio \
        -o /foss_fim/requirements.txt
        ```
    - When upgrading, you can can add a specific version to the package, but it may reject it if it creates compatiability issues.
    
4. `Compare / validation`:
The compile commands updated the /foss_fim/requirements.txt file. Compare the original git source version to the new file. Ensure key pacakges have not been lost. It is possible, but very rare, that one of our key packages other than numpy will downgrade. If one does downgrade, you can manually re-upgrade it in the steps below.
5. `Install the updated requirements.txt file`
    ```
    uv pip install --refresh -r /foss_fim/requirements.txt
    ```
    You should not get any errors as the uv pip compile ensured all pacakges were compatiable.
    As an optional safety check, you can run:
    ```
    uv pip check
    ```

### requirements.txt and testing

Each time you run commands to change packages and installed the updated `requirements.txt` and a some key tests are required.

### Testing Steps:
1) While still in the container, run your scripts, but also keep in mind:
    - Did the update trigger some packages that affected other scripts such as inundation or catfim? Test, or have help testing, those tools.
    - ALWAYS, regardless of what pacakges were changed, run a fim_pipeline test with a HUC or two. I generally run 12090301 as it has some alpha test data, then I can run alpha test on it quickly.
2) Close the current docker container.
3) Create a new test docker image, which will automatically include the updated `requirements.txt` file.
4) Retest the affected tools and re-test fim_pipline with a few HUCs like you did before.
5) Check it in, and advise the FIM DevOps team about a new docker image is coming.
6) Ensure the PR approver also includes a test docker build and test packages.

++++++++++++++++
## SUPER IMPORTANT -- requirements.txt versioning
Never allow any entry on the requirements.txt not have a version. This can
easily result in a package being automatically and accidently being upgraded on a future
Docker build.

----------------------------------------
## system_requirements.sh and environment configuration

This file sets a very large number of package, file i/o, GDAL, OGR, parallization optimization.
It also includes various configurations that help with memory and cpu management to help
stop memory or CPU overclocking. This helps with memory crashes, broken process pools, and
A wide range of file and memory issues.

Yes.. it has to be a .sh bash file and not an .env file.

This also impacts performance and optimations such as rasterio, geopandas, parquet, fiona, numpy
pyarrow, GDAL, OGR, C++, MPIEXEC (such as TauDEM) and a host of other geospatial packages.

To test changes to the file, you can make update to it, then test by running:
```
  . /foss_fim/system_requirements.sh   (yes.. you need the dot in front)
```

----------------------------------------
## Docker build example  (For non NOAA / OWP environments)
Here is an example of building the Docker image including some build logs to help identify problems.

```
(Adjusting for the inundation mapping code folder)
cd /home/rdp-user/projects/dev-python/inundation-mapping/

(Adjusting the image name for version number or test name)
clear ; proj_name="dev-fix-docker" ; image_name="fim_dev_team:docker-test-1" ; current_date=$(date +"%Y%m%d_%H%M");  mkdir -p /home/rdp-user/projects/docker_builds ; log_file_name="/home/rdp-user/docker_builds/build_${current_date}.log" ; docker buildx build --provenance=false --sbom=false --output=type=docker --no-cache -t ${image_name} -f /home/rdp-user/projects/${proj_name}/inundation-mapping/Dockerfile.dev . 2>&1 | tee ${log_file_name} ; echo "Done. See log file at ${log_file_name}"
```
Note: If you are running docker build a large number of times for debugging, occasionally run `docker builder prune` to cleanup previous Cocker builds.


## Dockerfile.owp

This section is applicable to NOAA / Fim-Dev teams only and is needed only when using tools in the internal OWP / Linux servers.

Add notes about how the owp enviros work.

Pending

Dockerfile.owp does not have a two stage build as the tools run in the owp environments do not need TauDEM. fim-pipeline.sh is not available for use in the OWP servers.



----------------------------------------
## TauDEM Acknowledgments & Credits

This project utilizes **TauDEM (Terrain Analysis Using Digital Elevation Models)**, developed by **David Tarboton** and the Hydrology Research Group at **Utah State University**. 

* **Source Code:** [TauDEM GitHub Repository](https://github.com)
* **Official Website:** [TauDEM Website](http://usu.edu)
* **License:** TauDEM is distributed under the GNU Public License (GPL).

### Academic Citation
> Tarboton, D. G., (2015), "TauDEM - Terrain Analysis Using Digital Elevation Models," Utah State University, http://usu.edu.
