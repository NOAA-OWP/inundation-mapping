FROM docker:20.10.2-dind

RUN apk add --no-cache python3 python3-dev py3-pip build-base openssl-dev libffi-dev git

ENV PYTHONUNBUFFERED 1
RUN mkdir -p /opt/connector
WORKDIR /opt/connector

COPY requirements.txt .
RUN pip3 install -r requirements.txt --no-cache-dir
COPY . /opt/connector

EXPOSE 6000

RUN chmod +x /opt/connector/entrypoint.sh
ENTRYPOINT ["/opt/connector/entrypoint.sh"]
