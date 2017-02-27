'use strict';

let pgp = require('pg-promise')();
let kms = require('aws-services-lib/aws_promise/kms');
//let sts = require('aws-services-lib/aws_promise/sts');
let s3 = require('aws-services-lib/aws_promise/s3bucket');

exports.handler = (event, context, callback) => {

  console.log('Received event:', JSON.stringify(event, null, 2));

  let bucketIAMRoleArn = process.env.BUCKET_IAM_ROLE_ARN;
  let bucketRegion = process.env.BUCKET_REGION;
  let kmsRegion = process.env.BUCKET_REGION;
  //let federateRoleArn = process.env.federateRoleArn;
  //let accountRoleArn = process.env.accountRoleArn;
  //let externalId = process.env.externalId;
  //let dynamoDBTableName = process.env.dynamoDBTableName;
  let redshiftConnectionString = process.env.REDSHIFT_CONNECTION_STRING;
  let redshiftUser = process.env.REDSHIFT_USER;
  let redshiftPass = process.env.REDSHIFT_PASS;

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
    var yearMonth = tokens[2].split('-')[0].substring(0, 6);
    var connection = null;
    var params = {
      region: kmsRegion,
      password: redshiftPass
    };
    kms.decrypt(params).then(function(data) {
      redshiftPass = data.Plaintext.toString();
      redshiftConnectionString = 'pg:' + redshiftUser + ':' + redshiftPass + '@' + redshiftConnectionString;
    }).then(function() {
      // get sqls first
      params = {
        bucket: bucket,
        key: key
      }
      return s3.getObject(params).then(function(data) {
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
    // do not export to dynamodb now and restore exporting when necessary
    /*}).then(function(result) {
      console.log("\n Now importing to DynamoDB");
      return exportToDynamoDB(connection, yearMonth, bucketRegion, dynamoDBTableName).then(function(result) {
        console.log(result);
        connection.client.end();
        callback(null, result);
      }).catch(function(err) {
        console.log(err);
        if (connection) connection.client.end();
        callback(err);
      });*/
    }).catch(function(err) {
      console.log(err);
      callback(err);
    });
  }
};

/*function exportToDynamoDB(connection, yearMonth, bucketRegion, dynamoDBTableName) {

  var AWS = require('aws-sdk');
  var docClient = new AWS.DynamoDB.DocumentClient({region: bucketRegion});

	var queryMaxEndDate = "select MAX(lineitem_usageenddate) \
		from AWSBilling<year_month> \
		where datediff(hour,cast(lineitem_usagestartdate as datetime),cast(lineitem_usageenddate as datetime)) = 1;"
	queryMaxEndDate = queryMaxEndDate.replace("<year_month>", yearMonth);

	var queryByAccount = "select lineItem_UsageAccountId, \
		sum(cast(lineItem_BlendedCost as float)) blended, \
		sum(cast(lineitem_unblendedcost as float)) unblended, \
		to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999990D00') blended_rounded, \
		to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999990D00') unblended_rounded \
		from AWSBilling<year_month> \
		group by lineItem_UsageAccountId \
		order by lineItem_UsageAccountId;"
	queryByAccount = queryByAccount.replace("<year_month>", yearMonth);

	var queryByAccountAndProduct = "select lineItem_UsageAccountId, \
		lineitem_productcode, \
		sum(cast(lineItem_BlendedCost as float)) blended, \
		sum(cast(lineitem_unblendedcost as float)) unblended, \
		to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999990D00') blended_rounded, \
		to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999990D00') unblended_rounded \
		from AWSBilling<year_month> \
		group by lineItem_UsageAccountId, lineitem_productcode \
		order by lineItem_UsageAccountId, lineitem_productcode;"
	queryByAccountAndProduct = queryByAccountAndProduct.replace("<year_month>", yearMonth);

	var lastEndDate = null;
	console.log("finding last end date");
  return connection.client.queryP(queryMaxEndDate).then(function(result) {
		console.log(result);
		lastEndDate = result.rows[0].max;
		return result;
	}).then(function(result) {
		console.log("storing costs by account");
	  return connection.client.queryP(queryByAccount).then(function(result) {
			var promises = [];
			result.rows.forEach(function(row) {
				row.lineitem_productcode = '%';
				row.hash_key = yearMonth + '.' + row.lineitem_usageaccountid + '.' + row.lineitem_productcode;
				row.range_key = yearMonth;
				row.account_key = yearMonth + '.' + row.lineitem_usageaccountid;
				row.product_key = yearMonth + '.' + row.lineitem_productcode;
				row.last_end_date = lastEndDate;
				//console.log(row);
				var params = {
					TableName : dynamoDBTableName,
					Item: row
				};
				promises.push(docClient.put(params).promise());
			});
			return Promise.all(promises).then(function(retArray) {
				return retArray;
			});
		});
	}).then(function(retArray) {
		console.log("storing costs by account & service");
		return connection.client.queryP(queryByAccountAndProduct).then(function(result) {
			var promises = [];
			result.rows.forEach(function(row) {
				row.hash_key = yearMonth + '.' + row.lineitem_usageaccountid + '.' + row.lineitem_productcode;
				row.range_key = yearMonth;
				row.account_key = yearMonth + '.' + row.lineitem_usageaccountid;
				row.product_key = yearMonth + '.' + row.lineitem_productcode;
				row.last_end_date = lastEndDate;
				//console.log(row);
				var params = {
					TableName : dynamoDBTableName,
					Item: row
				};
				promises.push(docClient.put(params).promise());
			});
			return Promise.all(promises).then(function(retArray) {
				return retArray;
			});
		});
  });
}
*/
