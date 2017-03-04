
var dateformat = require('dateformat');
var AWS = require('aws-sdk');
var metricsLib = new (require('./metrics'))();
var billing_data = require('./billing_data');
var graph_data = require('./graph_data');

module.exports = {
  generate: function(accountId, metrics, region, callback) {
    // find all the service metrics
    metricsLib.listEstimatedChargesMetrics(accountId, region).then(function(serviceNames) {
      return metricsLib.findLatestEstimatedChargesByServiceMetrics(accountId, serviceNames, region, new Date(metrics.estimated.Timestamp), 1).then(function(metricsArray) {
        var serviceMetrics = {};
        serviceNames.forEach(function(serviceName, idx) {
          serviceMetrics[serviceName] = metricsArray[idx][0];
        });
        return serviceMetrics;
      });
    }).then(function(serviceMetrics) {
      console.log(JSON.stringify(serviceMetrics, null, 2));
      billing_data.setConnection().then(function(data) {
        var params = {
          "accountId": accountId,
          "timestamp": metrics.estimated.Timestamp
        }
        var promises = [];
        promises.push(billing_data.buildAccountData(params));
        promises.push(billing_data.buildServiceData(params));
        return Promise.all(promises).then(function(retArray) {
          console.log("successfully completed to get account/service month data");
          // replace the current account month data with the Estimated Charges Metric Data
          retArray[0][0].sum[0].enddate = metrics.estimated.Timestamp;
          retArray[0][0].sum[0].blended = metrics.estimated.Maximum;
          retArray[0][0].sum[0].unblended = metrics.estimated.Maximum;
          // replace the current service month data with the Estimated Charges Metric Data
          console.log(JSON.stringify(retArray[1], null, 2));
          var currentMonth = (new Date(metrics.estimated.Timestamp).getMonth());
          //retArray[1][0].last_end_date = metrics.estimated.Timestamp;
          retArray[1][0].sum_by_line_item.forEach(function(item) {
            // if there month is different, don't overwrite
            var itemLatestMonth = (new Date(serviceMetrics[item.service].Timestamp)).getMonth();
            console.log(`itemLatestMonth[${itemLatestMonth}] - currentMonth[${currentMonth}] in service[${item.service}]`);
            if (itemLatestMonth != currentMonth) {
              console.log(`itemLatestMonth[${itemLatestMonth}] is different from currentMonth[${currentMonth}] in service[${item.service}]`);
            }
            else {
              item.blended = serviceMetrics[item.service].Maximum;
              item.unblended = serviceMetrics[item.service].Maximum;
              item.enddate = serviceMetrics[item.service].Timestamp;
            }
          });
          var monthData = {account: null, service: null};
          monthData.account = retArray[0];
          monthData.service = retArray[1];
          monthData.region = region;
          return monthData;
        }).catch(function(err) {
          console.log("failed to billing_data.buildServiceData : " + err);
          throw err;
        });
      }).then(function(monthData) {
        var asOfDateTime = new Date(monthData.account[0].last_end_date);
        var asOfDateStr = dateformat(asOfDateTime, "UTC:dd");
        var asOfTimeStr = dateformat(asOfDateTime, "UTC:HH:MM:ss");
        monthData.titles = {
          account: `<b>Account Charges As Of Day ${asOfDateStr} At ${asOfTimeStr} UTC In Each Month</b><br><i>Account : ${accountId}</i>`,
          service: `<b>Charges By Service In Each Month</b><br><i>Account : ${accountId}</i>`
        };
        graph_data.draw(monthData, function(err, data) {
          if (err) {
            console.log(err);
            callback(err);
          }
          else {
            console.log(data);
            callback(null, data);
          };
        });
      }).catch(function(err) {
        console.log("failed to metricsLib.findLatestSGASBillingMetrics : " + err);
        throw err;
      });
    });
  }
}
