
var AWS = require('aws-sdk');
var aws_watch_remote = new (require('aws-services-lib/aws/cloudwatch.js'))();
var aws_watch_local = new (require('aws-services-lib/aws/cloudwatch.js'))();

function Metrics() {

  this.remoteInput = {
    region: null
  };

  this.localInput = {
    region: null
  };

  this.callback = null;
  this.current = new Date();

  // metrics for EstimatedCharges
  this.AWSEstimatedChargesMetricQuery = {
    StartTime: null,
    EndTime: null,
    MetricName: 'EstimatedCharges',
    Namespace: 'AWS/Billing',
    Period: 60 * 60 * 4,
    Statistics: [
     'SampleCount', 'Average', 'Sum', 'Minimum', 'Maximum'
    ],
    Dimensions: [
      {
        Name: 'LinkedAccount',
        Value: null
      },
      {
        Name: 'Currency',
        Value: 'USD'
      }
   ],
   Unit: 'None'
  };

  // metrics for Increased data metrics
  this.SGASIncreasedMetricData = {
    MetricData: [
      {
        MetricName: 'IncreasedPercentages',
        Dimensions: [
          {
            Name: 'LinkedAccount',
            Value: null
          }
        ],
        Timestamp: null,
        Unit: 'Percent',
        Value: null
      },
      {
        MetricName: 'EstimatedCharges',
        Dimensions: [
          {
            Name: 'LinkedAccount',
            Value: null
          }
        ],
        Timestamp: null,
        Unit: 'None',
        Value: null
      }
    ],
    Namespace: 'SGASBilling'
  };

  // metrics for IncreasedPercentages Query
  this.IncreasedPercentagesMetricQuery = {
    StartTime: null,
    EndTime: null,
    MetricName: 'IncreasedPercentages',
    Namespace: 'SGASBilling',
    Period: 60 * 60 * 4,
    Statistics: [
     'SampleCount', 'Average', 'Sum', 'Minimum', 'Maximum'
    ],
    Dimensions: [
      {
        Name: 'LinkedAccount',
        Value: null
      }
   ],
   Unit: 'Percent'
  };

  var me = this;

  function buildAWSEstimatedChargesMetricQuery() {
    var startTime = new Date(me.current.getFullYear(), me.current.getMonth(), me.current.getDate());
    //me.current.setHours(me.current.getHours() - 1);
    startTime.setHours(startTime.getHours() - 24*14);
    me.AWSEstimatedChargesMetricQuery.StartTime = startTime;
    me.AWSEstimatedChargesMetricQuery.EndTime = me.current;
    me.AWSEstimatedChargesMetricQuery.Dimensions[0].Value = me.accountId;
    return me.AWSEstimatedChargesMetricQuery;
  }

  function buildEstimatedChargesMetricsData() {
    console.log('<<<Starting buildEstimatedChargesMetricsData...');
    metricQuery = buildAWSEstimatedChargesMetricQuery();
    me.remoteInput.metricQuery = metricQuery;
    //console.log(JSON.stringify(me.remoteInput));
    console.log('>>>...calling findMetricsStatistics in buildEstimatedChargesMetricsData');
    aws_watch_remote.findMetricsStatistics(me.remoteInput);
  }

  function buildIncreasedPercentagesMetricQuery() {
    console.log('<<<Starting buildIncreasedPercentagesMetricQuery...');
    var startTime = new Date(me.current.getFullYear(), me.current.getMonth(), me.current.getDate());
    //me.current.setMinutes(me.current.getMinutes() - 5);
    startTime.setHours(startTime.getHours() - 24);
    me.IncreasedPercentagesMetricQuery.StartTime = startTime;
    me.IncreasedPercentagesMetricQuery.EndTime = me.current;
    me.IncreasedPercentagesMetricQuery.Dimensions[0].Value = me.accountId;
    me.localInput.metricQuery = me.IncreasedPercentagesMetricQuery;
    //console.log(JSON.stringify(me.localInput));
    console.log('>>>...calling findMetricsStatistics in buildIncreasedPercentagesMetricQuery');
    aws_watch_local.findMetricsStatistics(me.localInput);
  }

  function buildIncreasedMetricsData() {

    console.log('<<<Starting buildIncreasedMetricsData...');
    //console.log(JSON.stringify(me.remoteInput));
    var metrics = me.remoteInput.metrics.sort(function(a, b){return b.Timestamp - a.Timestamp}).splice(0,2);
    console.log("***EST CHARGE METRICS : " + JSON.stringify(metrics));

    // check if the new metric data has been generated in remoteRegion
    var percentageMetrics = me.localInput.metrics;
    if (me.localInput.metrics && me.localInput.metrics.length >= 2) {
      percentageMetrics = me.localInput.metrics.sort(function(a, b){return b.Timestamp - a.Timestamp}).splice(0,2);
    }
    console.log("***PERCENTAGE METRICS : " + JSON.stringify(percentageMetrics));
    if (percentageMetrics && percentageMetrics[0] && metrics[0]) {
      console.log("percentage metrics time : " + percentageMetrics[0].Timestamp);
      console.log("est charge metrics time : " + metrics[0].Timestamp);
      if (percentageMetrics[0].Timestamp.getTime() >= metrics[0].Timestamp.getTime()) {
        console.log("no new EstimatedChargeds metric data, so just return");
        me.callback(null, true);
        return;
      }
    }

    var curEstimatedCharge = metrics[0].Maximum;
    var prevEstimatedCharge = 0;
    var increased = 0;
    var percentage = 0;
    var timeStamp = metrics[0].Timestamp;
    if (metrics.length >= 2) {
      prevEstimatedCharge = metrics[1].Maximum;
      increased = curEstimatedCharge - prevEstimatedCharge;
      if (prevEstimatedCharge > 0) {
        percentage = (increased / prevEstimatedCharge) * 100;
      }
    }

    //currentTime = new Date();
    metricData = me.SGASIncreasedMetricData;
    //metricData.MetricData[0].Timestamp = currentTime;
    metricData.MetricData[0].Timestamp = timeStamp;
    metricData.MetricData[0].Value = percentage;
    metricData.MetricData[0].Dimensions[0].Value = me.accountId;
    metricData.MetricData[1].Timestamp = timeStamp;
    metricData.MetricData[1].Value = curEstimatedCharge;
    metricData.MetricData[1].Dimensions[0].Value = me.accountId;
    me.localInput.metricData = metricData;
    //console.log(JSON.stringify(me.localInput));
    console.log(JSON.stringify(metricData));
    console.log('>>>...completed buildIncreasedMetricsData');
    aws_watch_local.addMetricData(me.localInput);
  }

  function succeeded(input) { console.log(input); me.callback(null, true); }
  function failed(input) { me.callback(null, false); }
  function errored(err) { me.callback(err, null); };

  me.addMetricData = function(accountId, creds, localRegion, remoteRegion, current, callback) {

    me.accountId = accountId;
    me.localInput.region = localRegion;
    me.localInput.metrics = null;
    me.remoteInput.region = remoteRegion;
    me.remoteInput.metrics = null;
    me.remoteInput.creds = creds;
    me.callback = callback;
    if (current)  me.current = current;

    var flows = [
      {func:buildEstimatedChargesMetricsData, success:aws_watch_remote.findMetricsStatistics, failure:failed, error:errored},
      {func:aws_watch_remote.findMetricsStatistics, success:buildIncreasedPercentagesMetricQuery, failure:failed, error:errored},
      {func:buildIncreasedPercentagesMetricQuery, success:aws_watch_local.findMetricsStatistics, failure:failed, error:errored},
      {func:aws_watch_local.findMetricsStatistics, success:buildIncreasedMetricsData, failure:buildIncreasedMetricsData, error:errored},
      {func:buildIncreasedMetricsData, success:aws_watch_local.addMetricData, failure:failed, error:errored},
      {func:aws_watch_local.addMetricData, success:succeeded, failure:failed, error:errored},
    ]
    aws_watch_remote.flows = flows;
    aws_watch_local.flows = flows;

    flows[0].func(me.remoteInput);
  }
}

module.exports = Metrics
