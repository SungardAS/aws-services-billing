
var AWS = require('aws-sdk');
var metricsLib = new (require('./metrics'))();
var generator = require('./alert_message_generator.js');

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
  if (alarmName.indexOf("OverIncreasedPercentagesAlarm-") < 0) {
    console.log('This alaram is not OverIncreasedPercentagesAlarm, so just return');
    context.done(null, true);
  }

  var dynamodb = new AWS.DynamoDB({region: region});

  var threshold = process.env.THRESHOLD_FOR_ALARMS;
  var allowedAverageCost = process.env.ALLOWED_AVERAGE_COST;
  var tableName = process.env.DYNAMODB_ALERT_TABLE_NAME;

  var current = new Date();
  metricsLib.findLatestSGASBillingMetrics(awsid, region, current, allowedAverageCost).then(function(metrics) {
    console.log(metrics);
    /*var average = metrics.average.Maximum;
    var estimated = metrics.estimated.Maximum;
    var cost = ((estimated - average) / average) * 100;
    if (cost <= allowedAverageCost) {
      console.log(`estimated [${estimated}] - average [${average}] cost [${cost}] is not greater than the allowed [${allowedAverageCost}], so no alert necessary`);*/
    if (metrics == null) {
      return context.done(null, true);
    }

    generator.generate(awsid, metrics, region, function(err, data) {
      if (err) {
        console.log("failed to generate alert message");
        return context.fail(err);
      }
      message = `EstimatedCharges Increase Percentage (${metrics.percentage}) is greater than Threshold (${threshold}).`;
      message += `\nAnd Percentage By Average (${metrics.cost}) is greater than Threshold (${allowedAverageCost}).`;
      message += "\n\nPlease review below graphs for more detail.";
      message += `\n${data.sum}`;
      message += `\n${data.service}`;
      var item = {
          "id": {"S": messageId},
          "awsid": {"S": awsid},
          "subject": {"S": subject},
          "message": {"S": message},
          "sentBy": {"S": sentBy},
          "sentAt": {"S": sentAt},
          //"createdAt": {"S": current.toISOString()},
          //"updatedAt": {"S": current.toISOString()},
          //"account": {"N": '0'},
          //"archivedBy": {"S": "none"}
      }
      console.log(item);
      var params = {
        "TableName": tableName,
        "Item" : item
      };
      dynamodb.putItem(params, function(err, data) {
        if (err) {
          console.log(err);
          context.fail(err, null);
        }
        else {
          context.done(null, true);
        }
      });
    }).catch(function(err) {
      console.log(err);
      context.fail(err);
    });
  });
}
