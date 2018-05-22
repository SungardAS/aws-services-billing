
var i = require('./index_import.js');
var event = {};
var context = {};

i.handler(event, context, function(err, data) {
  if (err)  {
    console.log(err);
  }
  else {
    console.log(data);
    if (!data) {
      console.log("failed to import billing data");
    }
    else {
      console.log("successfully completed to import billing data");
    }
  }
});
