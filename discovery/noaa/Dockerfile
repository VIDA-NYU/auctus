FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY discovery/noaa /usr/src/app/noaa
RUN pip install --no-deps -e noaa
CMD ["python", "-m", "noaa_discovery.noaa_discovery"]
