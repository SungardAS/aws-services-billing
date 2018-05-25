FROM node:8

RUN mkdir /app

WORKDIR /app

RUN git clone https://github.com/SungardAS/aws-services-billing.git

RUN cd aws-services-billing/src && npm install

CMD node /app/aws-services-billing/src/run_import.js
