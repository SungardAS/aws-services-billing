
var AWS = require('aws-sdk');
var metricsLib = new (require('./metrics'))();
var generator = require('./alert_message_generator.js');
var uuid = require('node-uuid');
var aws_cloudwatchlog = new (require('aws-services-lib/aws/cloudwatchlog.js'))();

var logGroupName = process.env.BILLING_LOG_GROUP_NAME;

exports.handler = function (event, context) {

  console.log(event.Records[0].Sns);
  /*
  { Type: 'Notification',
    MessageId: 'bcd9cfd3-8ccc-54e6-ae0f-9ec0455801f4',
    TopicArn: 'arn:aws:sns:us-east-1:266593598212:OverIncreasedPercentagesTopic',
    Subject: 'ALARM: "282307656817-OverIncreasedPercentagesAlarm" in US East - N. Virginia',
    Message: '{
      "AlarmName":"282307656817-OverIncreasedPercentagesAlarm",
      "AlarmDescription":"Alerted whenever the linked account\'s IncreasedPercentages[Sim] metric has new data.",
      "AWSAccountId":"266593598212",
      "NewStateValue":"ALARM",
      "NewStateReason":"Threshold Crossed: 1 datapoint (12.499999999999993) was greater than the threshold (10.0).",
      "StateChangeTime":"2017-02-07T13:10:44.738+0000",
      "Region":"US East - N. Virginia",
      "OldStateValue":"INSUFFICIENT_DATA",
      "Trigger":{
        "MetricName":"IncreasedPercentages",
        "Namespace":"SGASBilling",
        "Statistic":"MAXIMUM",
        "Unit":"Percent",
        "Dimensions":[{"name":"LinkedAccount","value":"282307656817"}],
        "Period":60,
        "EvaluationPeriods":1,
        "ComparisonOperator":"GreaterThanThreshold",
        "Threshold":10.0
      }
    }',
    Timestamp: '2017-02-07T13:10:44.779Z',
    SignatureVersion: '1',
    Signature: 'XlDtFlhZ+Ncyr+uzuAO+AIzMdtNKZBP2OPoSMAsctpyu83Xv1e2y1AS9g+pZQUfbQ6ujWX468Gcv905wKwJCvxNXvoTQzbksiLY2PKEWODGMq+dI8W2IllcTFn5rYjY3aQTUp5N8moqM6Pfki6jHshyTbvqt0QvT9GSWLv8gwaSmyv2eRE+pt94dZRtjHS0rHHriLryGBDRk6ENaXzg2aHST85QKGVXzKYp+oDMM72wdhGU/Z07CxpkjDHw3XFrS2oXc6OxnJvJj0lPErUghhO3C4SI6mxT6n6f6X8yd1JuvvHBmf8e1yjCjFrMdoiqIq6QAzFKtR8O6+CLZB+/2CA==',
    SigningCertUrl: 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem',
    UnsubscribeUrl: 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:266593598212:OverIncreasedPercentagesTopic:968f2960-1d59-4a46-af26-1549a3f3cbba',
    MessageAttributes: {} }
  */
  var message_json = JSON.parse(event.Records[0].Sns.Message);

  var region = event.Records[0].EventSubscriptionArn.split(":")[3];

  var messageId = event.Records[0].Sns.MessageId;
  var subject = event.Records[0].Sns.Subject;
  var message = message_json.NewStateReason;
  var sentBy = event.Records[0].Sns.TopicArn;
  var sentAt = event.Records[0].Sns.Timestamp;
  var awsid = null;
  var awsids = message_json.Trigger.Dimensions.filter(function(dimension) {
    return dimension.name == 'LinkedAccount';
  });
  if (awsids[0])  awsid = awsids[0].value;
  else awsid = message_json.AWSAccountId;
  var alarmName = message_json.AlarmName;
  var timestamp = message_json.StateChangeTime;

  var threshold = process.env.THRESHOLD_FOR_ALARMS;
  var allowedAverageCost = process.env.ALLOWED_AVERAGE_COST;

  var current = new Date();
  metricsLib.findLatestSGASBillingMetrics(awsid, region, current, allowedAverageCost).then(function(metrics) {
    console.log(metrics);
    if (metrics == null) {
      return context.done(null, true);
    }

    generator.generate(awsid, metrics, region, function(err, data) {
      if (err) {
        console.log("failed to generate alert message");
        return context.fail(err);
      }
      message = `There is a Spike in EstimatedCharges`;
      /*message += "\n\nPlease review below graphs for more detail.";
      message += `\n${data.sum}`;
      message += `\n${data.service}`;*/

      function succeeded(input) { context.done(null, true); }
      function failed(err) { context.fail(err, null); }
      function errored(err) { context.fail(err, null); }

      var logMessage = {
        "awsid": awsid,
        "subject": subject,
        "message": message,
        "images": [data.sum, data.service],
        "sentBy": sentBy,
        "sentAt": sentAt
      };

      var input = {
        region: region,
        groupName: logGroupName,
        streamName: timestamp.replace(/:/g, '') + "-" + uuid.v4(),
        //logMessage: event.Records[0].Sns.Message,
        logMessage: JSON.stringify(logMessage),
        //timestamp: (new Date(timestamp)).getTime()
        timestamp: (new Date()).getTime()
      };
      console.log(input);

      var flows = [
        {func:aws_cloudwatchlog.findLogGroup, success:aws_cloudwatchlog.findLogStream, failure:aws_cloudwatchlog.createLogGroup, error:errored},
        {func:aws_cloudwatchlog.createLogGroup, success:aws_cloudwatchlog.findLogStream, failure:failed, error:errored},
        //{func:aws_cloudwatchlog.findLogStream, success:aws_cloudwatchlog.findLogEvent, failure:aws_cloudwatchlog.createLogStream, error:errored},
        {func:aws_cloudwatchlog.findLogStream, success:aws_cloudwatchlog.createLogEvents, failure:aws_cloudwatchlog.createLogStream, error:errored},
        //{func:aws_cloudwatchlog.createLogStream, success:aws_cloudwatchlog.findLogEvent, failure:failed, error:errored},
        {func:aws_cloudwatchlog.createLogStream, success:aws_cloudwatchlog.createLogEvents, failure:failed, error:errored},
        //{func:aws_cloudwatchlog.findLogEvent, success:success_callback, failure:aws_cloudwatchlog.createLogEvents, error:errored},
        {func:aws_cloudwatchlog.createLogEvents, success:succeeded, failure:failed, error:errored}
      ]
      aws_cloudwatchlog.flows = flows;
      flows[0].func(input);
    });
  });
}
