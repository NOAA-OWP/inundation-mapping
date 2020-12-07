FROM docker:dind

RUN apk add --no-cache python3 python3-dev py3-pip

ENV PYTHONUNBUFFERED 1
RUN mkdir -p /opt/updater
WORKDIR /opt/updater

COPY requirements.txt .
RUN pip3 install -r requirements.txt --no-cache-dir
COPY . /opt/updater

RUN chmod +x /opt/updater/entrypoint.sh
ENTRYPOINT ["/opt/updater/entrypoint.sh"]
