FROM node:12-buster AS frontend-build

RUN mkdir /src
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json /src/
RUN npm install
#COPY frontend/prettier.config.js frontend/public frontend/README.md frontend/src frontend/tsconfig.json frontend/tslint.json /src/
COPY frontend /src/
RUN npm run build


FROM python:3.7

ENV TINI_VERSION v0.18.0
RUN curl -Lo /tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini && \
    chmod +x /tini

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python && /root/.poetry/bin/poetry config virtualenvs.create false
RUN pip --disable-pip-version-check install toml
COPY docker/install_deps.py poetry.lock /usr/src/app/
RUN python -c "import json, toml; json.dump(toml.load(open('poetry.lock')), open('poetry.lock.json', 'w'))"
RUN python3 install_deps.py poetry.lock.json
# CI: RUN pip --disable-pip-version-check install coverage==5.0.3
# CI: COPY .coveragerc /usr/src/app/

COPY lib_core lib_core
RUN pip --disable-pip-version-check install --no-deps -e /usr/src/app/lib_core
COPY lib_materialize lib_materialize
RUN pip --disable-pip-version-check install --no-deps -e /usr/src/app/lib_materialize
COPY coordinator coordinator
RUN pip --disable-pip-version-check install --no-deps -e /usr/src/app/coordinator
COPY --from=frontend-build /src/build /usr/src/app/coordinator/coordinator/frontend
RUN mv coordinator/coordinator/frontend/static/css/* coordinator/coordinator/static/css/ && \
    mv coordinator/coordinator/frontend/static/js/* coordinator/coordinator/static/js/ && \
    rm -rf coordinator/coordinator/frontend/static && \
    mv coordinator/coordinator/frontend/index.html coordinator/coordinator/templates/search.html && \
    mv coordinator/coordinator/frontend/* coordinator/coordinator/static/ && \
    rmdir coordinator/coordinator/frontend
ARG version
ENV DATAMART_VERSION ${version}
RUN test -n "${DATAMART_VERSION}"
ENTRYPOINT ["/tini", "--"]
CMD ["coordinator"]
