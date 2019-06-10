FROM python:3.6

WORKDIR /usr/src
RUN apt-get update && apt-get install -y maven default-jdk git && \
    rm -rf /var/lib/apt/lists/*
RUN git clone https://gitlab.com/ViDA-NYU/datamart/lazo-index-service.git
RUN cd lazo-index-service && ./install-repositories && mvn package

CMD java -jar /usr/src/lazo-index-service/target/lazo_index-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p ${LAZO_SERVER_PORT} -db elasticsearch -eh ${ELASTICSEARCH_HOST} -ep ${ELASTICSEARCH_PORT}