#!/bin/sh
source /home/ec2-user/aws-services-billing/.import.env.local

cd /home/ec2-user/aws-services-billing/src; source ~/.nvm/nvm.sh; nvm use 8; node run_import.js;
