FROM node:8

RUN mkdir /app

WORKDIR /app

COPY ./src /app/src

RUN cd /app/src && npm install

CMD node /app/src/run_import.js

