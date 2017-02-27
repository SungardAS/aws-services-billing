
//var uuid = require('node-uuid');
var AWS = require('aws-sdk');
//var aws_lambda = require('aws-services-lib/aws_promise/lambda.js');
var accountFinder = require('./account_finder');
var alarms = require('./alarms');
//var metrics = new (require('./metrics'))();

exports.handler = function (event, context) {

  console.log(JSON.stringify(event));
  var localRegion = event.region;
  console.log("localRegion = " + localRegion);
  var remoteRegion = 'us-east-1';

  //var threshold = process.env.THRESHOLD_FOR_ALARMS;
  var topicArn = process.env.TOPIC_ARN_FOR_ALARMS;
  var masterCreds = null;
  if (event.masterCredentials) {
    masterCreds = new AWS.Credentials({
      accessKeyId: event.masterCredentials.AccessKeyId,
      secretAccessKey: event.masterCredentials.SecretAccessKey,
      sessionToken: event.masterCredentials.SessionToken
    });
  }

  accountFinder.find({region:remoteRegion, credentials:masterCreds}).then(function(billingAccounts) {
    return billingAccounts;
  }).then(function(billingAccounts) {
    console.log(billingAccounts);
    console.log(billingAccounts.length + " accounts found");
    // find all metrics
    return alarms.findByNamePrefix({region: localRegion, ALARM_NAME_PREFIX: 'EstimatedChargesAlarm-', credentials:masterCreds}).then(function(data) {
      return [billingAccounts, data.MetricAlarms];
    });
  }).then(function(data) {
    var billingAccounts = data[0];
    var allAlarms = data[1];
    // setup alarms if not exist for each billing account
    var promises = [];
    billingAccounts.forEach(function(account) {
      // find this account's alarm is already created
      var alarmParams = {
        region: localRegion,
        accountId: account,
        topicArn: topicArn,
        metricsName: 'EstimatedCharges',
        metricsNameSpace: 'AWS/Billing',
        metricsUnit: 'None',
        //period: 60*60*6,  // 6 hours
        period: 60,
        threshold: 0,
        description: "Alerted whenever the linked account's EstimatedCharges is greater then threshold.",
        dimensions: [
          {
            Name: 'LinkedAccount',
            Value: account
          },
          {
            Name: 'Currency',
            Value: 'USD'
          }
        ],
        allAlarms: allAlarms,
        ALARM_NAME_PREFIX: 'EstimatedChargesAlarm-'
      }
      promises.push(alarms.setup(alarmParams));
    });
    return Promise.all(promises).then(function(retArray) {
      context.done(null, retArray);
    }).catch(function(err) {
      context.fail(err);
    });
  }).catch(function(err) {
    context.fail(err);
  })
}
