'use strict';

let kms = require('aws-services-lib/aws_promise/kms');
let pgp = require('pg-promise')();
let dateformat = require('dateformat');

module.exports = {

  post: function(params) {

    let kmsRegion = process.env.BUCKET_REGION;
    let redshiftConnectionString = process.env.REDSHIFT_CONNECTION_STRING;
    let redshiftUser = process.env.REDSHIFT_USER;
    let redshiftPass = process.env.REDSHIFT_PASS;

    var input = {
      region: kmsRegion,
      password: redshiftPass
    };
    return kms.decrypt(input).then(function(data) {
      redshiftPass = data.Plaintext.toString();
      redshiftConnectionString = 'pg:' + redshiftUser + ':' + redshiftPass + '@' + redshiftConnectionString;
    }).then(function() {
      // now run the sql in the redshift
      var connection = pgp(redshiftConnectionString);
      return connection.query(params.sql).then(function(result) {
  			console.log(result);
        pgp.end();
        return result;
      });
    });
  }
}
