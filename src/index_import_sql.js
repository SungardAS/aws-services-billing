'use strict';

var AWS = require('aws-sdk');

exports.handler = (event, context, callback) => {

  console.log('Received event:', JSON.stringify(event, null, 2));

  let region = process.env.AWS_DEFAULT_REGION;
  let sqlTableName = process.env.SQL_TABLE_NAME;

  // Get the object from the event and show its content type
  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
  var tokens = key.split('/');
  if (tokens[tokens.length-1].indexOf('.sql') < 0) {
    console.log("not an sql file, so just return");
    callback(null, null);
    return;
  }
  else {
    console.log("We've got a new billing file, " + key);
      // get sqls first
    params = {
      bucket: bucket,
      key: key
    }
    // save it to a dynamodb table
    var documentClient = new AWS.DynamoDB.DocumentClient({region: region});
    var params = {
      TableName : sqlTableName,
      Item: {
        "id": key,
        "bucket": bucket,
        "key": key,
        "sentAt": (new Date()).toISOString()
      }
    };
    return documentClient.put(params).promise().then(function(data) {
			console.log(data);
      return true;
		}).catch(function(err) {
      console.log("ignoring error during saving sql : " + err);
      callback(err);
    });
  }
};
