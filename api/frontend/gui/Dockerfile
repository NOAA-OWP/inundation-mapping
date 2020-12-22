FROM python:3.8.5-slim-buster

ENV PYTHONUNBUFFERED 1
RUN mkdir -p /opt/gui
WORKDIR /opt/gui

COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
COPY . /opt/gui

EXPOSE 5000

RUN chmod +x /opt/gui/entrypoint.sh
ENTRYPOINT ["/opt/gui/entrypoint.sh"]
