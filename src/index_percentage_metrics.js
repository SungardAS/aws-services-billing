
var alarms = require('./alarms');
var metricsLib = new (require('./metrics'))();
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
  var percentage = null;
  var average = null;
  var estimated = null;
  var current = new Date();
  alarms.setupOne(alarmParams).then(function(data) {
    return metricsLib.findLatestEstimatedChargesMetrics(accountId, region, current, 2).then(function(metrics) {
      console.log('completed to findLatestEstimatedChargesMetric in account[' + accountId + ']');
      console.log(metrics);
      //console.log(metric);
      latestMetrics = metrics;
      // find the increased percentage from the lastest 2 estimated charge metrics
      percentage = calculatePercentage(metrics);
      return metrics;
    }).catch(function(err) {
      console.log("failed to findLatestEstimatedChargesMetric in account[" + accountId + "] : " + err);
      return context.fail(err, null);
    });
  }).then(function(metrics) {
    var timestamp = current.toString();
    if (metrics != null && metrics.length > 0) {
      timestamp = metrics[0].Timestamp;
    }
    var params = {
      "accountId": accountId,
      "timestamp": timestamp
    }
    console.log("params to find average of last months : " + JSON.stringify(params));
    return billing_data.buildAccountData(params).then(function(data) {
      console.log("successfully completed to get account history data");
      average = findAccountAverage(data);
      return metrics;
    }).catch(function(err) {
      console.log("failed to billing_data.buildAccountData in account[" + accountId + "] : " + err);
      return context.fail(err, null);
    });
  }).then(function(metrics) {
    var estimated = 0;
    var timestamp = current.toString();
    if (metrics != null && metrics.length > 0) {
      estimated = metrics[0].Maximum;
      timestamp = metrics[0].Timestamp;
    }
    return metricsLib.addPercentageMetricData(accountId, region, percentage, average, estimated, timestamp).then(function(data) {
      console.log("successfully addPercentageMetricData");
      return context.done(null, data);
    }).catch(function(err) {
      console.log("failed to addPercentageMetricData in account[" + accountId + "] : " + err);
      return context.fail(err, null);
    });
  }).catch(function(err) {
    console.log("failed to alarms.setupOne in account[" + accountId + "] : " + err);
    return context.fail(err, null);
  });
}

function calculatePercentage(metrics) {

  if (metrics == null || metrics.length == 0) {
    console.log("there is no metrics found");
    return 0;
  }

  if (metrics.length == 1) {
    console.log("there is only one metrics found");
    return 0;
  }

  var curEstimatedCharge = metrics[0].Maximum;
  var prevEstimatedCharge = metrics[1].Maximum;
  var percentage = 0;
  var increased = curEstimatedCharge - prevEstimatedCharge;
  if (prevEstimatedCharge > 0) {
    percentage = (increased / prevEstimatedCharge) * 100;
  }
  return percentage;
}


function findAccountAverage(monthData) {
  // find the average of previous months' charges first
  var total = 0;
  monthData.forEach(function(data) {
    total += parseFloat(data.sum[0].unblended);
  });
  return total/monthData.length;
}
