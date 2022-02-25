FROM python:3.8 AS geo-data

RUN mkdir /usr/src/app
COPY lib_geo /usr/src/app/lib_geo
RUN pip --disable-pip-version-check --no-cache-dir install /usr/src/app/lib_geo
ENV DATAMART_GEO_DATA /usr/src/app/geo_data
RUN python -m datamart_geo --update /usr/src/app/geo_data && \
    ls -l /usr/src/app/geo_data

FROM python:3.8 AS sources
# If only there was a way to do this copy directly with Docker...
# https://github.com/moby/moby/issues/33551
RUN mkdir /usr/src/app
COPY lib_core /usr/src/app/lib_core
COPY lib_fslock /usr/src/app/lib_fslock
COPY lib_geo /usr/src/app/lib_geo
COPY lib_materialize /usr/src/app/lib_materialize
COPY lib_augmentation /usr/src/app/lib_augmentation
COPY lib_profiler /usr/src/app/lib_profiler
COPY apiserver /usr/src/app/apiserver
COPY coordinator /usr/src/app/coordinator
COPY profiler /usr/src/app/profiler
COPY cache_cleaner /usr/src/app/cache_cleaner
COPY snapshotter /usr/src/app/snapshotter
COPY discovery/test_discovery.py /usr/src/app/discovery/
COPY discovery/noaa /usr/src/app/discovery/noaa
COPY discovery/isi /usr/src/app/discovery/isi
COPY discovery/socrata /usr/src/app/discovery/socrata
COPY discovery/zenodo /usr/src/app/discovery/zenodo
COPY discovery/ckan /usr/src/app/discovery/ckan
COPY discovery/worldbank /usr/src/app/discovery/worldbank
COPY discovery/uaz_indicators /usr/src/app/discovery/uaz_indicators
COPY tests/data /usr/src/app/tests/data

FROM python:3.8

COPY --from=geo-data /usr/src/app/geo_data /usr/src/app/geo_data

ENV TINI_VERSION v0.18.0
RUN curl -Lo /tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini && \
    chmod +x /tini

ENV PYTHONFAULTHANDLER=1

RUN mkdir -p /usr/src/app/home && \
    useradd -d /usr/src/app/home -s /usr/sbin/nologin -u 998 appuser && \
    chown appuser /usr/src/app/home
WORKDIR /usr/src/app
RUN pip --disable-pip-version-check --no-cache-dir install toml
COPY docker/install_deps.py poetry.lock /usr/src/app/
RUN python install_deps.py poetry.lock  # NOTCI
# CI: RUN python install_deps.py --dev poetry.lock
# CI: RUN pip --disable-pip-version-check install coverage==5.5
# CI: COPY docker/coveragerc /usr/src/app/.coveragerc

COPY --chown=appuser --from=sources /usr/src/app /usr/src/app/
RUN sh -c "pip --disable-pip-version-check --no-cache-dir install --no-deps \$(for pkg in \"\$@\"; do printf -- \" -e ./%s\" \$pkg; done)" -- \
    lib_core lib_fslock lib_geo lib_materialize lib_augmentation lib_profiler \
    apiserver coordinator profiler cache_cleaner snapshotter \
    discovery/noaa discovery/isi discovery/isi discovery/socrata \
    discovery/zenodo discovery/ckan discovery/worldbank discovery/uaz_indicators

ENV DATAMART_GEO_DATA /usr/src/app/geo_data

RUN python -m compileall /usr/src/app/
ARG version
ENV DATAMART_VERSION ${version}
RUN test -n "${DATAMART_VERSION}"
USER 998
ENTRYPOINT [ \
    "/tini", "--", "/bin/bash", "-c", \
    "if [ x\"$OTEL_TRACES_EXPORTER\" != x ]; then OTEL_RESOURCE_ATTRIBUTES=service.name=$AUCTUS_OTEL_SERVICE OTEL_PYTHON_TORNADO_EXCLUDED_URLS=/health exec opentelemetry-instrument \"$@\"; else exec \"$@\"; fi", "--"]
CMD ["false"]
