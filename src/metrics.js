
var AWS = require('aws-sdk');

function Metrics() {

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

  this.AWSEstimatedChargesMetricsListQuery = {
    Dimensions: [
      {
        Name: 'LinkedAccount',
        Value: null
      },
      {
        Name: 'ServiceName',
        //Value: 'AWSCloudTrail'
      },
      {
        Name: 'Currency',
        Value: 'USD'
      }
    ],
    MetricName: 'EstimatedCharges',
    Namespace: 'AWS/Billing',
    //NextToken: 'STRING_VALUE'
  };

  this.AWSEstimatedChargesByServiceMetricQuery = {
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
        Name: 'ServiceName',
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
        MetricName: 'AverageCharges',
        Dimensions: [
          {
            Name: 'LinkedAccount',
            Value: null
          }
        ],
        Timestamp: null,
        Unit: 'None',
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

  this.SGASBillingMetricQuery = {
    StartTime: null,
    EndTime: null,
    MetricName: null,
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
   Unit: null
  };

  var me = this;

  me.findLatestEstimatedChargesMetrics = function(accountId, region, current, metricsCount) {
    var params = {
      region: region
    };
    var cloudwatch = new AWS.CloudWatch(params);
    var metrics = {'estimated': null, 'average': null, 'percentage': null};
    var startTime = new Date(current.getFullYear(), current.getMonth(), current.getDate());
    startTime.setHours(startTime.getHours() - 24*14);
    var metricQuery = JSON.parse(JSON.stringify(me.AWSEstimatedChargesMetricQuery));
    metricQuery.StartTime = startTime;
    metricQuery.EndTime = current;
    metricQuery.Dimensions[0].Value = accountId;
    return cloudwatch.getMetricStatistics(metricQuery).promise().then(function(data) {
      data.Datapoints.sort(function(a, b){return b.Timestamp - a.Timestamp});
      return data.Datapoints.splice(0, metricsCount);
    });
  }

  me.listEstimatedChargesMetrics = function(accountId, region) {
    var params = {
      region: region
    };
    var cloudwatch = new AWS.CloudWatch(params);
    var metricListQuery = JSON.parse(JSON.stringify(me.AWSEstimatedChargesMetricsListQuery));
    metricListQuery.Dimensions[0].Value = accountId;
    return cloudwatch.listMetrics(metricListQuery).promise().then(function(data) {
      /*
      {
        "ResponseMetadata": {
          "RequestId": "694edcba-0057-11e7-ba01-1587457dbbee"
        },
        "Metrics": [
          {
            "Namespace": "AWS/Billing",
            "MetricName": "EstimatedCharges",
            "Dimensions": [
              {
                "Name": "ServiceName",
                "Value": "AmazonES"
              },
              {
                "Name": "Currency",
                "Value": "USD"
              },
              {
                "Name": "LinkedAccount",
                "Value": "089476987273"
              }
            ]
          },
          ...
        ]
      }
      */
      var services = [];
      data.Metrics.forEach( metric => { services.push(metric.Dimensions[0].Value); } );
      return services;
    });
  }

  me.findLatestEstimatedChargesByServiceMetrics = function(accountId, serviceNames, region, current, metricsCount) {
    var params = {
      region: region
    };
    var cloudwatch = new AWS.CloudWatch(params);
    var startTime = new Date(current.getFullYear(), current.getMonth(), current.getDate());
    //me.current.setHours(me.current.getHours() - 1);
    startTime.setHours(startTime.getHours() - 24*14);
    var promises = [];
    serviceNames.forEach(function(serviceName) {
      var serviceMetricQuery = JSON.parse(JSON.stringify(me.AWSEstimatedChargesByServiceMetricQuery));
      serviceMetricQuery.StartTime = startTime;
      serviceMetricQuery.EndTime = current;
      serviceMetricQuery.Dimensions[0].Value = accountId;
      serviceMetricQuery.Dimensions[1].Value = serviceName;
      promises.push(cloudwatch.getMetricStatistics(serviceMetricQuery).promise().then(function(data) {
              data.Datapoints.sort(function(a, b){return b.Timestamp - a.Timestamp});
              return data.Datapoints.splice(0, metricsCount);
            }));
    });
    return Promise.all(promises);
  }

  me.addPercentageMetricData = function(accountId, region, percentage, average, estimatedCharge, timeStamp) {
    var params = {
      region: region
    };
    var cloudwatch = new AWS.CloudWatch(params);
    var metricData = JSON.parse(JSON.stringify(me.SGASIncreasedMetricData));
    metricData.MetricData[0].Timestamp = timeStamp
    metricData.MetricData[0].Value = percentage;
    metricData.MetricData[0].Dimensions[0].Value = accountId;
    metricData.MetricData[1].Timestamp = timeStamp;
    metricData.MetricData[1].Value = average;
    metricData.MetricData[1].Dimensions[0].Value = accountId;
    metricData.MetricData[2].Timestamp = timeStamp;
    metricData.MetricData[2].Value = estimatedCharge;
    metricData.MetricData[2].Dimensions[0].Value = accountId;
    console.log(metricData);
    return cloudwatch.putMetricData(metricData).promise();
  }

  me.findLatestSGASBillingMetrics = function(accountId, region, current) {
    var params = {
      region: region
    };
    var cloudwatch = new AWS.CloudWatch(params);
    var startTime = new Date(current.getFullYear(), current.getMonth(), current.getDate());
    startTime.setHours(startTime.getHours() - 24*14);
    var metricQuery = JSON.parse(JSON.stringify(me.SGASBillingMetricQuery));
    metricQuery.StartTime = startTime;
    metricQuery.EndTime = current;
    metricQuery.Dimensions[0].Value = accountId;
    console.log(JSON.stringify(metricQuery));
    metricQuery.MetricName = "IncreasedPercentages";
    metricQuery.Unit = "Percent";
    return cloudwatch.getMetricStatistics(metricQuery).promise().then(function(data) {
      data.Datapoints.sort(function(a, b){return b.Timestamp - a.Timestamp});
      var latestMetrics = {};
      latestMetrics.percentage = data.Datapoints[0];
      return latestMetrics;
    }).then(function(latestMetrics) {
      metricQuery.MetricName = "AverageCharges";
      metricQuery.Unit = "None";
      return cloudwatch.getMetricStatistics(metricQuery).promise().then(function(data) {
        data.Datapoints.sort(function(a, b){return b.Timestamp - a.Timestamp});
        latestMetrics.average = data.Datapoints[0];
        return latestMetrics;
      });
    }).then(function(latestMetrics) {
      metricQuery.MetricName = "EstimatedCharges";
      metricQuery.Unit = "None";
      return cloudwatch.getMetricStatistics(metricQuery).promise().then(function(data) {
        data.Datapoints.sort(function(a, b){return b.Timestamp - a.Timestamp});
        latestMetrics.estimated = data.Datapoints[0];
        return latestMetrics;
      });
    })
  }
}

module.exports = Metrics
