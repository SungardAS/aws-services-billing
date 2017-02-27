
var dateformat = require('dateformat');
var AWS = require('aws-sdk');
var metricsLib = new (require('./metrics'))();
var billing_data = require('./billing_data');
var graph_data = require('./graph_data');

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
  var current = new Date();
  var alarmName = message_json.AlarmName;
  var timestamp = message_json.StateChangeTime;
  if (alarmName.indexOf("OverIncreasedPercentagesAlarm-") < 0) {
    console.log('This alaram is not OverIncreasedPercentagesAlarm, so just return');
    context.done(null, true);
  }

  var cloudwatch = new AWS.CloudWatch({region: region});
  var dynamodb = new AWS.DynamoDB({region: region});

  var allowedPercentage = process.env.ALLOWED_PERCENTAGE;
  var tableName = process.env.DYNAMODB_ALERT_TABLE_NAME;

  //var startTime = new Date(current.getFullYear(), current.getMonth(), current.getDate());
  //startTime.setHours(startTime.getHours() - 12);
  //metricsLib.AWSEstimatedChargesMetricQuery.StartTime = startTime;
  //metricsLib.AWSEstimatedChargesMetricQuery.EndTime = current;
  var endTime = new Date(timestamp);
  var startTime = new Date(timestamp);
  startTime.setHours(startTime.getHours() - 12);
  metricsLib.AWSEstimatedChargesMetricQuery.StartTime = startTime;
  metricsLib.AWSEstimatedChargesMetricQuery.EndTime = endTime;
  metricsLib.AWSEstimatedChargesMetricQuery.Dimensions[0].Value = awsid;

  var monthData = {account: null, service: null};
  var input = {
    region: region,
    metricQuery: metricsLib.AWSEstimatedChargesMetricQuery
  }
  //console.log(JSON.stringify(input));
  cloudwatch.getMetricStatistics(metricsLib.AWSEstimatedChargesMetricQuery).promise().then(function(data) {
    //console.log(data);
    var metric = data.Datapoints.sort(function(a, b){return b.Timestamp - a.Timestamp}).splice(0,1)[0];
    console.log(metric);
    return metric;
  }).then(function(metric) {
    var params = {
      "accountId": "445750067739",
      "metricData": metric
    }
    return billing_data.buildAccountData(params).then(function(data) {
      console.log("successfully completed to get account month data");
      //console.log(JSON.stringify(data, null, 2));
      // replace the current month data with the Estimated Charges Metric Data
      data[0].sum[0].enddate = metric.Timestamp;
      data[0].sum[0].blended = metric.Maximum;
      data[0].sum[0].unblended = metric.Maximum;
      // check the given EstimatedCharges with the average of previous months' charges
      // if the given Estimated Charges is below than (average+average*0.02), set the notification ON
      var average = findAccountAverage(data);
      if (metric.Maximum > (average + (average*allowedPercentage))) {
        monthData.account = data;
        return params;
      }
      else {
        console.log(`EstimatedCharges "${metric.Maximum}" is close to the average "${average}", so no notification necessary`);
        return null;
      }
    }).catch(function(err) {
      console.log("err:" + err);
    });
  }).then(function(params) {
    if (params == null) return context.done(null, true);
    return billing_data.buildServiceData(params).then(function(data) {
      console.log("successfully completed to get service month data");
      //console.log(JSON.stringify(data, null, 2));
      monthData.service = data;
      monthData.region = region;
      //console.log(JSON.stringify(monthData, null, 2));
      graph_data.draw(monthData, function(err, data) {
        if (err) {
          console.log(err);
          context.fail(err, null);
        }
        else {
          console.log(data);
          var asOfDateTime = new Date(monthData.account[0].last_end_date);
          var asOfDateStr = dateformat(asOfDateTime, "UTC:dd");
          var asOfTimeStr = dateformat(asOfDateTime, "UTC:HH:MM:ss");
          // now, save the notification
          message += "\n\nPlease review below graphs for more detail.";
          message += `\n\nAccount Charges As Of Day ${asOfDateStr} At ${asOfTimeStr} UTC In Each Month`;
          message += `\n${data.sum}`;
          message += `\n\nCharges By Service In Each Month`;
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
        }
      })
    }).catch(function(err) {
      console.log("err:" + err);
    });
  }).catch(function(err) {
    console.log(err);
    context.fail(err);
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
