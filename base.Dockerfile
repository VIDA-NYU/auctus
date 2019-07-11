FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock
# CI: RUN pip install coverage

# This is the common part of the Dockerfiles
# It is copied in all of them, and this file is used for the CI
