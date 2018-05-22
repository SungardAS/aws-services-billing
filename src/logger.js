
// https://alexanderpaterson.com/posts/node-logging-like-a-boss-using-winston-and-aws-cloudwatch

var winston = require('winston'),
    CloudWatchTransport = require('winston-aws-cloudwatch');

var LOG_GROUP_NAME = "/SungardAS/billing/import";
var LOG_STREAM_NAME = new Date().toISOString().replace(/:/g, "-");

const logger = new winston.Logger({
  transports: [
    new (winston.transports.Console)({
      timestamp: true,
      colorize: true,
    })
  ]
});

var config = {
  logGroupName: LOG_GROUP_NAME,
  logStreamName: LOG_STREAM_NAME,
  createLogGroup: false,
  createLogStream: true,
  awsConfig: {
    //accessKeyId: process.env.CLOUDWATCH_ACCESS_KEY_ID,
    //secretAccessKey: process.env.CLOUDWATCH_SECRET_ACCESS_KEY,
    region: process.env.AWS_DEFAULT_REGION
  },
  formatLog: function (item) {
    //return item.level + ': ' + item.message + ' ' + JSON.stringify(item.meta)
    return item.level + ': ' + item.message
  }
}

logger.add(CloudWatchTransport, config);

logger.level = process.env.LOG_LEVEL || "silly";

logger.stream = {
  write: function(message, encoding) {
    logger.info(message);
  }
};

module.exports = logger;
