
var alarms = require('./alarms');
var metrics = new (require('./metrics'))();
var billing_data = require('./billing_data');

exports.handler = function (event, context) {

  console.log(JSON.stringify(event));

  var messageId = event.Records[0].Sns.MessageId;
  var subject = event.Records[0].Sns.Subject;
  var sentBy = event.Records[0].Sns.TopicArn;
  var sentAt = event.Records[0].Sns.Timestamp;
  var message_json = JSON.parse(event.Records[0].Sns.Message);
  var accountId = null;
  var accountIds = message_json.Trigger.Dimensions.filter(function(dimension) {
    return dimension.name == 'LinkedAccount';
  });
  if (accountIds[0])  accountId = accountIds[0].value;
  var alarmName = message_json.AlarmName;
  if (alarmName.indexOf("EstimatedChargesAlarm-") < 0) {
    console.log('This alaram is not EstimatedChargesAlarm, so just return');
    context.done(null, true);
  }

  var region = process.env.AWS_DEFAULT_REGION;
  var threshold = process.env.THRESHOLD_FOR_ALARMS;
  var topicArn = process.env.TOPIC_ARN_FOR_ALARMS;

  // find this account's alarm is already created
  var alarmParams = {
    region: region,
    accountId: accountId,
    topicArn: topicArn,
    metricsName: 'IncreasedPercentages',
    metricsNameSpace: 'SGASBilling',
    metricsUnit: 'Percent',
    period: 60*60, // 1 hour
    description: "Alerted whenever the linked account's IncreasedPercentages metric value is greater then threshold.",
    dimensions: [
      {
        Name: 'LinkedAccount',
        Value: accountId
      }
    ],
    threshold: threshold,
    ALARM_NAME_PREFIX: 'OverIncreasedPercentagesAlarm-'
  };
  alarms.setupOne(alarmParams).then(function(data) {
    var curEstimatedChargesMetric = null;
    metrics.findLatestEstimatedChargesMetric(accountId, region, new Date(), function(err, metric) {
      if(err) {
        console.log("failed to findLatestEstimatedChargesMetric in account[" + accountId + "] : " + err);
        return context.fail(err, null);
      }
      console.log('completed to findLatestEstimatedChargesMetric in account[' + accountId + ']');
      //console.log(metric);
      curEstimatedChargesMetric = metric;
      var params = {
        "accountId": accountId,
        "metricData": curEstimatedChargesMetric
      }
      billing_data.buildAccountData(params).then(function(data) {
        console.log("successfully completed to get account history data");
        //console.log(JSON.stringify(data, null, 2));
        // check the given EstimatedCharges with the average of previous months' charges
        // if the given Estimated Charges is below than (average+average*0.02), set the notification ON
        var average = findAccountAverage(data);
        var percentage = ((average - curEstimatedChargesMetric.Maximum) / curEstimatedChargesMetric.Maximum) * 100;
        metrics.addPercentageMetricData(accountId, region, percentage, curEstimatedChargesMetric.Maximum, curEstimatedChargesMetric.TimeStamp, function(err, metric) {
          if(err) {
            console.log("failed to addPercentageMetricData in account[" + accountId + "] : " + err);
            return context.fail(err, null);
          }
          context.done(null, metric);
        });
      }).catch(function(err) {
        console.log("failed to addPercentageMetricData in account[" + accountId + "] : " + err);
        return context.fail(err, null);
      });
    });
  }).catch(function(err) {
    console.log("failed to alarms.setupOne in account[" + accountId + "] : " + err);
    return context.fail(err, null);
  });
}

function findAccountAverage(monthData) {
  // find the average of previous months' charges first
  var total = 0;
  monthData.forEach(function(data) {
    total += parseFloat(data.sum[0].unblended);
  });
  return total/monthData.length;
}
