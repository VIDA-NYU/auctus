FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock docker/install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
# CI: RUN pip install coverage==4.5.4

# This is the common part of the Dockerfiles
# It is copied in all of them, and this file is used for the CI
