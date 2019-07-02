FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
ARG version
ENV DATAMART_VERSION ${version}
RUN echo x${DATAMART_VERSION}x && test -n "${DATAMART_VERSION}"

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY discovery/socrata /usr/src/app/socrata
RUN pip install --no-deps -e /usr/src/app/socrata
CMD ["python", "-m", "socrata_discovery"]
