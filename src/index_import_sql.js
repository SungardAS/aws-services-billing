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
    var documentClient = new AWS.DynamoDB.DocumentClient({region: region});
    var params = {
      TableName: sqlTableName,
      FilterExpression: 'attribute_not_exists(processedAt)'
    };
    return documentClient.scan(params).promise().then(function(data) {
      if (data.Items.length > 0) {
        console.log(JSON.stringify(data.Items));
        console.log("We have unprocessed " + data.Items.length + " sql jobs, so set it 'skip'");
        var itemToProcess = data.Items[0];
        itemToProcess['processedAt'] = 'skip';
        var params = {
          TableName : sqlTableName,
          Item: itemToProcess
        };
        return documentClient.put(params).promise().then(function(data) {
          console.log("billing job '" + itemToProcess['key'] + "' has been set to be skipped");
          return true;
        }).catch(function(err) {
          console.log("failed in setting an existing job as 'skip' : " + err);
          return callback(err);
        });
      }
      else {
        return true;
      }
    }).then(function(data) {
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
  			console.log("new sql job '" + key + "'has been successfully stored");
        return true;
  		}).catch(function(err) {
        console.log("failed in saving a new sql job: " + err);
        callback(err);
      });
    }).catch(function(err) {
      console.log("failed in getting existing sql jobs : " + err);
      callback(err);
    });
  }
};
