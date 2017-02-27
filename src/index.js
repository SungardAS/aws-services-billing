
var controllerHandler = require('aws-services-lib/lambda/controller_handler.js')

exports.handler = (event, context) => {
  controllerHandler.handler(event, context);
}

controllerHandler.allocate_controller = function(path) {
  var controller = require('./' + path + '_controller');
  return controller;
}
