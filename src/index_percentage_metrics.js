
var alarms = require('./alarms');
var metrics = new (require('./metrics'))();

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
  metrics.addMetricData(accountId, null, region, region, new Date(), function(err, data) {
    if(err) {
      console.log("failed to add metrics in account[" + accountId + "] : " + err);
      context.fail(err, null);
    }
    else {
      console.log('completed to add metrics in account[' + accountId + ']');
      console.log(data);
      alarms.setupOne(alarmParams).then(function(data) {
        context.done(null, data);
      }).catch(function(err) {
        context.fail(err);
      });
    }
  });
}
