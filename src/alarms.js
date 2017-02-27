
var AWS = require('aws-sdk');

module.exports = {

  setup: function(params) {
    //console.log(params);
    var accountAlarms = params.allAlarms.filter(function(alarm) {
      return alarm.AlarmName == params.ALARM_NAME_PREFIX + params.accountId;
    });
    if (accountAlarms.length == 0) {
      return this.add(params).then(function(data) {
        console.log("Created Alarm for account " + params.accountId);
        return data;
      });
    }
    else {
      if (accountAlarms[0].Threshold == params.threshold) {
        console.log("Alarm found for account " + params.accountId + ", so just return");
        Promise.resolve(true);
      }
      else {
        return this.add(params).then(function(data) {
          console.log("Updated Threshold of Alarm for account " + params.accountId);
          return data;
        });
      }
    }
  },

  setupOne: function(params) {
    var self = this;
    console.log(params);
    return self.find(params).then(function(data) {
      console.log(data);
      if (data.MetricAlarms.length == 0) {
        return self.add(params).then(function(data) {
          console.log("Created Alarm for account " + params.accountId);
          return data;
        });
      }
      else {
        if (data.MetricAlarms[0].Threshold == params.threshold) {
          console.log("Alarm found for account " + params.accountId + ", so just return");
          Promise.resolve(true);
        }
        else {
          return self.add(params).then(function(data) {
            console.log("Updated Threshold of Alarm for account " + params.accountId);
            return data;
          });
        }
      }
    }).catch(function(err) {
      throw err;
    });
  },

  findByNamePrefix: function(params) {
    var cloudwatch = new AWS.CloudWatch({region:params.region});
    var input = {
      //ActionPrefix: 'STRING_VALUE',
      AlarmNamePrefix: params.ALARM_NAME_PREFIX,
      /*AlarmNames: [
        'STRING_VALUE',
      ],*/
      //MaxRecords: 0,
      //NextToken: 'STRING_VALUE',
      //StateValue: 'OK | ALARM | INSUFFICIENT_DATA'
    };
    return cloudwatch.describeAlarms(input).promise();
  },

  find: function(params) {
    var cloudwatch = new AWS.CloudWatch({region:params.region});
    var input = {
      MetricName: params.metricsName,
      Namespace: params.metricsNameSpace,
      Dimensions: [
        {
          Name: 'LinkedAccount',
          Value: params.accountId
        }
      ],
      Unit: params.metricsUnit
    };
    return cloudwatch.describeAlarmsForMetric(input).promise();
    /*
    { ResponseMetadata: { RequestId: '1b97dfdd-ee6e-11e6-86be-fd25241a476c' },
      MetricAlarms:
      [ {
        AlarmName: 'OverIncreasedPercentagesAlarm-089476987273-',
        AlarmArn: 'arn:aws:cloudwatch:us-east-1:089476987273:alarm:OverIncreasedPercentagesAlarm-089476987273-',
        AlarmDescription: 'Alerted whenever the linked account\'s IncreasedPercentages[Sim] metric has new data.',
        AlarmConfigurationUpdatedTimestamp: Wed Feb 08 2017 20:16:47 GMT-0600 (CST),
        ActionsEnabled: true,
        OKActions: [],
        AlarmActions: [Object],
        InsufficientDataActions: [],
        StateValue: 'INSUFFICIENT_DATA',
        StateReason: 'Unchecked: Initial alarm creation',
        StateUpdatedTimestamp: Wed Feb 08 2017 20:16:47 GMT-0600 (CST),
        MetricName: 'IncreasedPercentages',
        Namespace: 'SGASBilling',
        Statistic: 'Maximum',
        Dimensions: [Object],
        Period: 60,
        Unit: 'Percent',
        EvaluationPeriods: 1,
        Threshold: 10,
        ComparisonOperator: 'GreaterThanThreshold'
      }]
    }
    */
  },

  add: function(params) {
    var input = {
      AlarmName: params.ALARM_NAME_PREFIX + params.accountId,
      ComparisonOperator: 'GreaterThanThreshold',
      EvaluationPeriods: 1,
      MetricName: params.metricsName,
      Namespace: params.metricsNameSpace,
      Period: params.period,
      Threshold: params.threshold,
      ActionsEnabled: true,
      AlarmActions: [
        params.topicArn
      ],
      AlarmDescription: params.description,
      Dimensions: params.dimensions,
      //ExtendedStatistic: 'STRING_VALUE',
      InsufficientDataActions: [
      ],
      OKActions: [
      ],
      Statistic: 'Maximum',
      Unit: params.metricsUnit
    };
    var cloudwatch = new AWS.CloudWatch({region:params.region});
    return cloudwatch.putMetricAlarm(input).promise();
  },

  deleteAll: function(params) {
    return this.findByNamePrefix(params).then(function(data) {
      return data.MetricAlarms;
    }).then(function(allAlarms) {
      console.log(allAlarms.length + " alarms found");
      var names = [];
      allAlarms.forEach(function(alarm){
        names.push(alarm.AlarmName);
      });
      var input = {
        AlarmNames: names
      };
      console.log(input);
      var cloudwatch = new AWS.CloudWatch({region:params.region});
      return cloudwatch.deleteAlarms(input).promise();
    })
  }
}
