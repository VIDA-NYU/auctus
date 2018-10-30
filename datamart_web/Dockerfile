FROM node:8

RUN mkdir -p /usr/src/app/datamart_web
WORKDIR /usr/src/app/datamart_web

#COPY package*.json .
COPY . /usr/src/app/datamart_web
RUN npm install &&\
    npm run build &&\
    rm -rf node_modules &&\
    npm install -g serve

EXPOSE 5000

CMD ["serve", "-s", "/usr/src/app/datamart_web/build"]
