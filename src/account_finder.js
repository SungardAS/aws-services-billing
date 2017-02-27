
var AWS = require('aws-sdk');

module.exports = {

  find: function(params) {
    var cloudwatch = new AWS.CloudWatch(params);
    params = {
      Dimensions: [
        {
          Name: 'LinkedAccount',
          //Value: 'STRING_VALUE'
        },
      ],
      MetricName: 'EstimatedCharges',
      Namespace: 'AWS/Billing',
      NextToken: null
    };
    var accounts = [];
    return add(cloudwatch, params, accounts).then(function(data) {
      return data;
    });
  }
}

function add(cloudwatch, params, accounts) {
  //console.log(params);
  return cloudwatch.listMetrics(params).promise().then(function(data) {
    data.Metrics.forEach(function(metrics) {
      //console.log(JSON.stringify(metrics.Dimensions));
      metrics.Dimensions.forEach(function(dim) {
        if (dim.Name == "LinkedAccount" && accounts.indexOf(dim.Value) < 0) {
          accounts.push(dim.Value);
        }
      });
    });
    return data.NextToken;
  }).then(function(nextToken) {
    //console.log("nextToken: " + nextToken);
    if (nextToken) {
      params.NextToken = nextToken;
      return add(cloudwatch, params, accounts);
    }
    else {
      return accounts;
    }
  });
}
