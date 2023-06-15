FROM ghcr.io/osgeo/gdal:ubuntu-small-3.7.0

RUN apt-get update && apt-get install -y \
    python3-pip

## install python 3 modules ##
COPY Pipfile .
COPY Pipfile.lock .
RUN pip3 install pipenv==2022.4.8 && PIP_NO_CACHE_DIR=off pipenv install --system --deploy --ignore-pipfile