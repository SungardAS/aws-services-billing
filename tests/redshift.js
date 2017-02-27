'use strict';

let kms = require('../lib/aws_promise/kms');
let pgp = require('pg-promise')();
let dateformat = require('dateformat');

module.exports = {

  findUsageCharges: function(configFileName, accountId, endDateString) {

    let fs = require("fs");
    let data = fs.readFileSync(__dirname + '/json/' + configFileName, {encoding:'utf8'});
    let data_json = JSON.parse(data);

    let kmsRegion = data_json.kmsRegion;
    let redshiftConnectionString = data_json.redshiftConnectionString;
    let redshiftUser = data_json.redshiftUser;
    let redshiftPass = data_json.redshiftPass;
    console.log('data_json:', data_json);

    let endDate = new Date(endDateString);
    let prevMonthEndDate = new Date(endDate.setMonth(endDate.getMonth()-1));
    var yearMonth = dateformat(new Date(prevMonthEndDate), 'yyyymm');
    if (endDate.getMonth() === prevMonthEndDate.getMonth()) {
        // this case is the prev month has shorter days than the given endDate month,
        // so the generated prevMonthEndDate and endDate has the same month
        // for example,
        // if endDate is Oct. 31, the prevMonthEndDate will be Oct. 1 because Sep has only 30 days
        // so we need to arrange the month
        let prevFirstDayInMonth = new Date(endDate.getFullYear(), endDate.getMonth()-1, 1);
        yearMonth = dateformat(new Date(prevFirstDayInMonth), 'yyyymm');
    }
    let endDateinISOString = new Date(prevMonthEndDate).toISOString();
    var querySum = "select max(lineitem_usagestartdate) enddate, \
      sum(cast(lineitem_unblendedcost as float)) unblended, \
      sum(cast(lineitem_blendedcost as float)) blended \
      from AWSBilling<year_month> \
      where datediff(hour,cast(lineitem_usagestartdate as datetime),cast(lineitem_usageenddate as datetime)) = 1 \
      and lineitem_usageenddate <= '<usage_end_date>' \
      and lineitem_usageaccountid = '<account>';"
    querySum = querySum.replace("<year_month>", yearMonth);
    querySum = querySum.replace("<usage_end_date>", endDateinISOString);
    querySum = querySum.replace("<account>", accountId);
    console.log('querySum:', querySum);

    var connection = null;
    var input = {
      region: kmsRegion,
      password: redshiftPass
    };
    return kms.decrypt(input).then(function(data) {
      redshiftPass = data.Plaintext.toString();
      redshiftConnectionString = 'pg:' + redshiftUser + ':' + redshiftPass + '@' + redshiftConnectionString;
      console.log('completed to build redshiftConnectionString');
    }).then(function() {
      // connect to the redshift
      connection = pgp(redshiftConnectionString);
      console.log("We've got a connection");
      return connection.query(querySum).then(function(result) {
        console.log(result);
        return result;
      });
    }).then(function(data) {
      console.log(data);
      pgp.end();
      return data
    }).catch(function(err) {
      console.log("error : " + err);
      pgp.end();
      throw new Excecption(err);
    });
  }
}
