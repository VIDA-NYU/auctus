FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
RUN pip install requests sodapy
COPY lib_core lib_core
RUN pip install -e /usr/src/app/lib_core
COPY discovery/web /usr/src/app/web
RUN pip install -e web
CMD ["python", "-m", "web_discovery"]
