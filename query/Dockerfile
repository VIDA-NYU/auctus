FROM python:3.6 as java

WORKDIR /usr/src
RUN apt-get update && apt-get install -y maven default-jdk git && \
    rm -rf /var/lib/apt/lists/*
RUN git clone https://gitlab.com/ViDA-NYU/datamart/scdp.git
RUN cd scdp && mvn package


FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install aio-pika elasticsearch
RUN apt-get update && apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*
COPY lib_core lib_core
RUN pip install -e /usr/src/app/lib_core
COPY lib_materialize lib_materialize
RUN pip install -e /usr/src/app/lib_materialize
COPY lib_profiler lib_profiler
RUN pip install -e /usr/src/app/lib_profiler
COPY --from=java /usr/src/scdp/target/SCDP-0.1-jar-with-dependencies.jar \
    lib_profiler/datamart_profiler/scdp.jar
COPY query query
RUN pip install -e /usr/src/app/query
CMD ["query"]
