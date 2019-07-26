# FIXME: SCDP currently disabled
#FROM python:3.6 as java
#
#WORKDIR /usr/src
#RUN apt-get update && apt-get install -y maven default-jdk git && \
#    rm -rf /var/lib/apt/lists/*
#RUN git clone https://gitlab.com/ViDA-NYU/datamart/scdp.git
#RUN cd scdp && mvn package


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

#RUN apt-get update && apt-get install -y default-jre && \
#    rm -rf /var/lib/apt/lists/*
COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY lib_materialize lib_materialize
RUN pip install --no-deps -e /usr/src/app/lib_materialize
COPY lib_profiler lib_profiler
RUN pip install --no-deps -e /usr/src/app/lib_profiler
COPY profiler profiler
#COPY --from=java /usr/src/scdp/target/SCDP-0.1-jar-with-dependencies.jar \
#    lib_profiler/datamart_profiler/scdp.jar
RUN pip install --no-deps -e /usr/src/app/profiler
ENTRYPOINT ["/tini", "--"]
CMD ["profiler"]
