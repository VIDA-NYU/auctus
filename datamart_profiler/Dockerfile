FROM python:3.6 as java

WORKDIR /usr/src
RUN apt-get update && apt-get install -y maven default-jdk git && \
    rm -rf /var/lib/apt/lists/*
RUN git clone https://gitlab.com/ViDA-NYU/datamart/scdp.git
RUN cd scdp && mvn package


FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*
RUN pip install aio-pika elasticsearch
RUN pip install numpy pandas
COPY datamart_core datamart_core
RUN pip install -e /usr/src/app/datamart_core
COPY datamart_profiler datamart_profiler
COPY --from=java /usr/src/scdp/target/SCDP-0.1-jar-with-dependencies.jar \
    scdp.jar
RUN pip install -e /usr/src/app/datamart_profiler
CMD ["datamart_profiler"]
