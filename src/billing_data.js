
var dateformat = require('dateformat');
var kms = require('aws-services-lib/aws_promise/kms');
var pgp = require('pg-promise')();
//var plotly = require('./index_plotly.js');
//var slack = require('./index_slack.js');

//var bucketName = process.env.S3_BUCKET_NAME;
var kmsRegion = process.env.KMS_REGION;
var redshiftConnectionString = process.env.REDSHIFT_CONNECTION_STRING;
var redshiftUser = process.env.REDSHIFT_USER;
var redshiftPass = process.env.REDSHIFT_PASS;

var startYearMonthStr = "201608";

//var accountId = null;
var connection = null;

var querySum = "select lineItem_UsageAccountId, \
  MAX(lineitem_usageenddate) enddate, \
  to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999990D00') blended, \
  to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999990D00') unblended \
  from AWSBilling<year_month> \
  where lineitem_usageenddate <= '<usage_end_date>' \
  and lineitem_usageaccountid = '<account>' \
  and datediff(hour,cast(lineitem_usagestartdate as datetime),cast(lineitem_usageenddate as datetime)) = 1 \
  group by lineItem_UsageAccountId \
  order by lineItem_UsageAccountId;"

var queryServiceSum = "select lineItem_UsageAccountId, lineitem_productcode service, \
  MAX(lineitem_usageenddate) enddate, \
  to_char(sum(cast(lineItem_BlendedCost as float)), 'FM999990D00') blended, \
  to_char(sum(cast(lineitem_unblendedcost as float)), 'FM999990D00') unblended \
  from AWSBilling<year_month> \
  where lineitem_usageenddate <= '<usage_end_date>' \
  and lineitem_usageaccountid = '<account>' \
  and datediff(hour,cast(lineitem_usagestartdate as datetime),cast(lineitem_usageenddate as datetime)) = 1 \
  group by lineItem_UsageAccountId, lineitem_productcode \
  order by lineItem_UsageAccountId, lineitem_productcode;"

module.exports = {

  buildAccountData: function(params) {
    var lastEndDate = new Date(params.metricData.Timestamp);
    var yearMonth = dateformat(lastEndDate, 'yyyymm');
    querySum = querySum.replace("<account>", params.accountId);
    return setConnection().then(function(data) {
      //connection = pgp(connectionString);
      var promises = [];
      var prevMonthcount = 0;
      var monthData = findMonthDate(lastEndDate, prevMonthcount);
      while(monthData.year_month >= startYearMonthStr) {
        promises.push(findMonthSum(monthData));
        monthData = findMonthDate(lastEndDate, ++prevMonthcount);
      }
      return Promise.all(promises).then(function(retArray) {
        pgp.end();
        //console.log(JSON.stringify(retArray));
        return retArray;
      }).catch(function(err) {
        pgp.end();
        throw err;
      });
    }).catch(function(err) {
      pgp.end();
      throw err;
    });
  },

  buildServiceData: function(params) {
    var lastEndDate = new Date(params.metricData.Timestamp);
    var yearMonth = dateformat(lastEndDate, 'yyyymm');
    queryServiceSum = queryServiceSum.replace("<account>", params.accountId);
    return setConnection().then(function(data) {
      //connection = pgp(connectionString);
      var promises = [];
      var prevMonthcount = 0;
      var monthData = findMonthDate(lastEndDate, prevMonthcount);
      while(monthData.year_month >= startYearMonthStr) {
        promises.push(findMonthSumByService(monthData));
        monthData = findMonthDate(lastEndDate, ++prevMonthcount);
      }
      return Promise.all(promises).then(function(retArray) {
        pgp.end();
        //console.log(JSON.stringify(retArray));
        return retArray;
      }).catch(function(err) {
        pgp.end();
        throw err;
      });
    }).catch(function(err) {
      pgp.end();
      throw err;
    });
  }
}

function setConnection() {
  if (connection) return Promise.resolve();
  var input = {
    region: kmsRegion,
    password: redshiftPass
  };
  return kms.decrypt(input).then(function(data) {
    var password = data.Plaintext.toString();
    var connectionString = 'pg:' + redshiftUser + ':' + password + '@' + redshiftConnectionString;
    connection = pgp(connectionString);
    return true;
  });
}

function findMonthDate(dateStr, prevMonthcount) {
  // first the same day of the previous month
  var lastEndDate = new Date(dateStr);
  lastEndDate = lastEndDate.setMonth(lastEndDate.getMonth() - prevMonthcount);
  var lastEndDateStr = new Date(lastEndDate).toISOString().replace(".000Z", "Z");
  // now find the previous month-year
  var firstDay = new Date(dateStr);
  firstDay = new Date(firstDay.getFullYear(), firstDay.getMonth(), 1);
  firstDay = firstDay.setMonth(firstDay.getMonth() - prevMonthcount);
  var yearMonth = dateformat(new Date(firstDay), 'yyyymm');
  var ret = {first_day:firstDay, year_month:yearMonth, last_end_date:lastEndDateStr};
  return ret;
}

function findMonthSum(monthData) {
  var querySumForMonth = querySum.replace("<year_month>", monthData.year_month).replace("<usage_end_date>", monthData.last_end_date);
  //console.log(querySumForMonth);
  return connection.query(querySumForMonth).then(function(result) {
    //console.log(result);
    monthData.sum = result;
    return monthData;
  });
}

function findMonthSumByService(monthData) {
  var queryServiceSumForMonth = queryServiceSum.replace("<year_month>", monthData.year_month).replace("<usage_end_date>", monthData.last_end_date);
  //console.log(queryServiceSumForMonth);
  return connection.query(queryServiceSumForMonth).then(function(result) {
    //console.log(result);
    monthData.sum_by_line_item = result;
    return monthData;
  });
}
