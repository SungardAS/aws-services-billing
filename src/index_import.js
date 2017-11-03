'use strict';

let AWS = require('aws-sdk');
let pgp = require('pg-promise')();
let kms = require('aws-services-lib/aws_promise/kms');

exports.handler = (event, context, callback) => {

  console.log('Received event:', JSON.stringify(event, null, 2));

  let region = process.env.AWS_DEFAULT_REGION;
  let kmsRegion = process.env.BUCKET_REGION;
  let bucketIAMRoleArn = process.env.BUCKET_IAM_ROLE_ARN;
  let bucketRegion = process.env.BUCKET_REGION;
  let sqlTableName = process.env.SQL_TABLE_NAME;
  let redshiftConnectionString = process.env.REDSHIFT_CONNECTION_STRING;
  let redshiftUser = process.env.REDSHIFT_USER;
  let redshiftPass = process.env.REDSHIFT_PASS;

  // Get the sql from the dynamodb
  var documentClient = new AWS.DynamoDB.DocumentClient({region: region});
  var params = {
    TableName: sqlTableName,
    FilterExpression: 'attribute_not_exists(processedAt)'
  };
  return documentClient.scan(params).promise().then(function(data) {
    console.log(data.Items);
    if (data.Items.length == 0) {
      console.log("no new sql, so just return");
      callback(null, null);
      return false;
    }
    else {
      console.log("We've got a new billing file, " + data.Items[0]);
      var tokens = data.Items[0].id.split('/');
      var itemToProcess = data.Items[0];
      var yearMonth = tokens[2].split('-')[0].substring(0, 6);
      var connection = null;
      var params = {
        region: kmsRegion,
        password: redshiftPass
      };
      return kms.decrypt(params).then(function(data) {
        redshiftPass = data.Plaintext.toString();
        redshiftConnectionString = 'pg:' + redshiftUser + ':' + redshiftPass + '@' + redshiftConnectionString;
      }).then(function() {
        // get sqls first
        var s3 = new AWS.S3({region: region});
        params = {
          Bucket: data.Items[0].bucket,
          Key: data.Items[0].key
        }
        return s3.getObject(params).promise().then(function(data) {
          //console.log(data.Body.toString());
          var sqlStr = data.Body.toString().replace('<AWS_ROLE>', bucketIAMRoleArn).replace("<S3_BUCKET_REGION>", "'" + bucketRegion + "'");
          console.log(sqlStr);
          return sqlStr;
        }).catch(function(err) {
          console.log(err);
          callback(err);
        });
      }).then(function(sqlStr) {
        // get the connection to redshit
        connection = pgp(redshiftConnectionString);
        // drop the current month table first if exists
        var redshiftDropTableSqlString = "drop table AWSBilling<Year_Month>; drop table AWSBilling<Year_Month>_tagMapping;";
        redshiftDropTableSqlString = redshiftDropTableSqlString.replace("<Year_Month>", yearMonth).replace("<Year_Month>", yearMonth);
        console.log("dropping existing billing tables : " + redshiftDropTableSqlString);
        return connection.query(redshiftDropTableSqlString).then(function(result) {
    			console.log(result);
          return sqlStr;
    		}).catch(function(err) {
          console.log("ignoring error during dropping tables : " + err);
          return sqlStr;
        });
      }).then(function(sqlStr) {
        // now run the sql in the redshift
    		console.log("importing billing data");
        return connection.query(sqlStr).then(function(result) {
    			console.log(result);
          pgp.end();
          return result;
        }).catch(function(err) {
          console.log(err);
          pgp.end();
          callback(err);
        });
      }).then(function(sqlStr) {
        // set this item processed
        itemToProcess['processedAt'] = (new Date()).toISOString();
        var params = {
          TableName : sqlTableName,
          Item: itemToProcess
        };
        return documentClient.put(params).promise().then(function(data) {
          console.log(data);
          return true;
        }).catch(function(err) {
          console.log(err);
          callback(err);
        });
      }).catch(function(err) {
        console.log(err);
        callback(err);
      });
    }
  }).catch(function(err) {
    console.log(err);
    callback(err);
  })
};
