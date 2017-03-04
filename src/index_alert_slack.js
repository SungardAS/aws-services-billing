
var AWS = require('aws-sdk');
const url = require('url');
const https = require('https');
var metricsLib = new (require('./metrics'))();
var generator = require('./alert_message_generator.js');

// The base-64 encoded, encrypted key (CiphertextBlob) stored in the HOOK_URL environment variable
const slackWebHookUrl = process.env.HOOK_URL;
// The Slack channel to send a message to stored in the SLACK_CHANNEL environment variable
const slackChannel = process.env.SLACK_CHANNEL;
var hookUrl;

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

  var allowedAverageCost = process.env.ALLOWED_AVERAGE_COST;
  var tableName = process.env.DYNAMODB_ALERT_TABLE_NAME;

  var current = new Date();
  metricsLib.findLatestSGASBillingMetrics(awsid, region, current).then(function(metrics) {
    console.log(metrics);
    var average = metrics.average.Maximum;
    var estimated = metrics.estimated.Maximum;
    var cost = ((estimated - average) / average) * 100;
    if (cost <= allowedAverageCost) {
      console.log(`estimated [${estimated}] - average [${average}] cost [${cost}] is not greater than the allowed [${allowedAverageCost}], so no alert necessary`);
      return context.done(null, true);
    }
    generator.generate(awsid, metrics, region, function(err, data) {
      if (err) {
        console.log("failed to generate alert message");
        return context.fail(err);
      }
      message = buildMessage(awsid, data);
      if (hookUrl) {
        // Container reuse, simply process the event with the key in memory
        processEvent(message, context);
      }
      else if (slackWebHookUrl && slackWebHookUrl !== '') {
        const encryptedBuf = new Buffer(slackWebHookUrl, 'base64');
        const cipherText = { CiphertextBlob: encryptedBuf };
        const kms = new AWS.KMS({region:process.env.KMS_REGION});
        kms.decrypt(cipherText, (err, data) => {
          if (err) {
            console.log('Decrypt error:', err);
            return context.fail(err);
          }
            hookUrl = `https://${data.Plaintext.toString('ascii')}`;
            processEvent(message, context);
        });
      }
      else {
        return context.fail('Hook URL has not been set.');
      }
      context.done(null, true);
    }).catch(function(err) {
      console.log(err);
      context.fail(err);
    });
  });
}

function postMessage(message, callback) {
  const body = JSON.stringify(message);
  console.log(hookUrl);
  const options = url.parse(hookUrl);
  options.method = 'POST';
  options.headers = {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
  };
  const postReq = https.request(options, (res) => {
    const chunks = [];
    res.setEncoding('utf8');
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => {
      if (callback) {
        callback({
          body: chunks.join(''),
          statusCode: res.statusCode,
          statusMessage: res.statusMessage,
        });
      }
    });
    return res;
  });

  postReq.write(body);
  postReq.end();
}

function processEvent(slackMessage, callback) {
  slackMessage.channel = slackChannel;
  postMessage(slackMessage, (response) => {
    if (response.statusCode < 400) {
      console.info('Message posted successfully');
      callback.done(null, null);
    } else if (response.statusCode < 500) {
      console.error(`Error posting message to Slack API: ${response.statusCode} - ${response.statusMessage}`);
      callback.done(null, null);  // Don't retry because the error is due to a problem with the request
    } else {
      // Let Lambda retry
      callback.fail(`Server error when processing message: ${response.statusCode} - ${response.statusMessage}`);
    }
  });
}

function buildMessage(accountId, data) {
  var message = {
    icon_emoji: ":postbox:",
    "text": "New Billing Alert!",
    "attachments": [
        {
            "text": "Peak In Estimated Charges Has Been Detected.",
            "color": "warning",
            "fields": [
              {
                  "title": "Account Id",
                  "value": accountId,
                  "short": true
              }
            ],
            "author_name": "Sungard Availability Services",
            "footer": "Created By SungardAS/aws-services",
            "footer_icon": "https://raw.githubusercontent.com/SungardAS/aws-services-lib/master/docs/images/logo.png",
            //"ts": new Date()

        },
        {
            "image_url": data.sum,
            "color": "warning"
        },
        {
            "image_url": data.service,
            "color": "warning"
        }
    ]
  }
  return message;
}
