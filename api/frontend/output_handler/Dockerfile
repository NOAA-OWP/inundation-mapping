FROM python:3.8.5-slim-buster

ENV PYTHONUNBUFFERED 1
RUN mkdir -p /opt/output_handler
WORKDIR /opt/output_handler

COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
COPY . /opt/output_handler

RUN chmod +x /opt/output_handler/entrypoint.sh
ENTRYPOINT ["/opt/output_handler/entrypoint.sh"]
