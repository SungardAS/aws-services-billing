
var uuid = require('node-uuid');
var dateformat = require('dateformat');
var kms = require('aws-services-lib/aws_promise/kms');

var username = process.env.PLOTLY_USERNAME;
var apiKey = null;

module.exports = {

  draw: function(params, callback) {
    var input = {
      region: params.region,
      password: process.env.PLOTLY_API_KEY
    };
    kms.decrypt(input).then(function(data) {
      apiKey = data.Plaintext.toString();
      var sumData = buildSumData(params.account);
      var sumByServiceData = buildServiceData(params.service);
      var sumGraphUrl = null;
      var sumByServiceGraphUrl = null;
      buildGraph(sumData, function(err, data) {
        if(err) return callback(err, null);
        else {
          sumGraphUrl = data;
          console.log(`sum graph url : ${sumGraphUrl}`);
          buildGraph(sumByServiceData, function(err, data) {
            if(err) return callback(err, null);
            else {
              sumByServiceGraphUrl = data;
              console.log(`service graph url : ${sumByServiceGraphUrl}`);
              var graphUrls = {sum: sumGraphUrl, service: sumByServiceGraphUrl};
              //console.log(graphUrls);
              /*sendSlackMessage(graphUrls, function(err, data) {
                if(err) return callback(err, null);
                else {
                  return callback(null, data);
                }
              });*/
              callback(null, graphUrls);
            }
          });
        }
      });
    }).catch(function(err) {
      callback(err);
    });
  }
}

function buildGraph(data, callback) {
  var plotly = require('plotly')(username, apiKey);
  var figure = { 'data': data };
  //console.log(figure);
  var imgOpts = {
      format: 'png',
      width: 1000,
      height: 600
  };
  plotly.getImage(figure, imgOpts, function (error, imageStream) {
    if (error) {
      console.log("failed to getImage : " + error);
      throw error;
    }
    var AWS = require('aws-sdk');
    var keyName = 'graphs/' + uuid.v4() + '.png';
    var s3obj = new AWS.S3({"params": {Bucket: process.env.S3_BUCKET_NAME, Key: keyName, ACL:"public-read"}});
    //console.log(s3obj);
    s3obj.upload({Body: imageStream}).
      on('httpUploadProgress', function(evt) {
        console.log(evt);
      }).
      send(function(err, data) {
        if (err) {
          console.log(err, err.stack);
          throw err;
        }
        else {
          /*
            { ETag: '"40ac5bb2fa0644f3c118c99965fcb9d1"',
              Location: 'https://s3.amazonaws.com/sgas.sam/myKey2.png',
              key: 'myKey2.png',
              Key: 'myKey2.png',
              Bucket: 'sgas.sam' }
          */
          //console.log(data);
          //console.log(data.Location);
          callback(null, data.Location);
        }
      });
  });
}

function buildSumData(monthDataArray) {
  var blended = {
    x: [],
    y: [],
    //mode: "lines+markers",
    name: "'blended'",
    //line: {shape: "linear"},
    type: "bar"
  };
  var unblended = {
    x: [],
    y: [],
    //mode: "lines+markers",
    name: "'unblended'",
    //line: {shape: "linear"},
    type: "bar"
  };
  for(var i = monthDataArray.length-1; i >= 0; i--) {
    //console.log(monthDataArray[i]);
    blended.x.push(dateformat(new Date(monthDataArray[i].first_day), 'yyyy-mm'));
    blended.y.push(monthDataArray[i].sum[0].blended);
    unblended.x.push(dateformat(new Date(monthDataArray[i].first_day), 'yyyy-mm'));
    unblended.y.push(monthDataArray[i].sum[0].unblended);
  }
  return [blended, unblended];
}

function buildServiceData(monthDataArray) {

  var dates = {};
  for(var i = monthDataArray.length-1; i >= 0; i--) {
    //console.log(monthDataArray[i].sum_by_line_item);
    var date = null;
    monthDataArray[i].sum_by_line_item.forEach(function(item) {
      if(date == null) {
        date = item.enddate;
      }
      else if (date < item.enddate) {
        date = item.enddate;
      }
    });
    var line_items = monthDataArray[i].sum_by_line_item;
    dates[date] = [];
    line_items.forEach(function(item) {
      dates[date].push({service: item.service, blended: item.blended, unblended: item.unblended});
    });
  }

  var data = [];
  var trace = {
    x: [],
    y: [],
    //mode: "lines+markers",
    name: "",
    //line: {shape: "linear"},
    type: "bar"
  };
  Object.keys(dates).forEach(function(key) {
    var dateTrace = JSON.parse(JSON.stringify(trace));
    dates[key].forEach(function(data) {
      dateTrace.x.push(data.service);
      dateTrace.y.push(data.unblended);
      dateTrace.name = `'${key}'`;
    });
    data.push(dateTrace);
  });

  return data;
}
