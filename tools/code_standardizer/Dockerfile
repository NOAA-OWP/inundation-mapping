FROM python:3.8.5-slim-buster

ENV PYTHONUNBUFFERED 1
RUN mkdir -p /opt/code_standardizer
WORKDIR /opt/code_standardizer

COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
COPY . /opt/code_standardizer

RUN chmod +x /opt/code_standardizer/entrypoint.sh
ENTRYPOINT ["/opt/code_standardizer/entrypoint.sh"]
