FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock docker/install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
# CI: RUN pip install coverage
# CI: COPY .coveragerc /usr/src/app/
ARG version
ENV DATAMART_VERSION ${version}
RUN echo x${DATAMART_VERSION}x && test -n "${DATAMART_VERSION}"

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY lib_materialize lib_materialize
RUN pip install --no-deps -e /usr/src/app/lib_materialize
COPY coordinator coordinator
RUN pip install --no-deps -e /usr/src/app/coordinator
ENTRYPOINT ["/tini", "--"]
CMD ["coordinator"]
