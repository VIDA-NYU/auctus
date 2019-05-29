FROM python:3.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY Pipfile.lock install_deps.py /usr/src/app/
RUN python3 install_deps.py Pipfile.lock

COPY lib_core lib_core
RUN pip install --no-deps -e /usr/src/app/lib_core
COPY lib_materialize lib_materialize
RUN pip install --no-deps -e /usr/src/app/lib_materialize
COPY lib_augmentation lib_augmentation
RUN pip install --no-deps -e /usr/src/app/lib_augmentation
COPY coordinator coordinator
RUN pip install --no-deps -e /usr/src/app/coordinator
CMD ["coordinator"]
