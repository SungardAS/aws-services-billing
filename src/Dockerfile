FROM node:8

RUN mkdir /app

WORKDIR /app

COPY ./*.* /app/

RUN cd /app && npm install

CMD node /app/run_import.js
