
var fs = require('fs');

var i = require('./index_import.js');
var event = {};
var context = {};

var logFilepath = `../logs/${(new Date()).toISOString()}.log`;

i.handler(event, context, function(err, data) {
  if (err)  {
    console.log(err);
  }
  else {
    console.log("successfully completed to import billing data");
    console.log(data);
    fs.writeFileSync(logFilepath, `successfully completed to import billing data: ${data}\n`);
  }
});
