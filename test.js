require('dotenv').load()
var lambdaFunctions = require('./lambda')
lambdaFunctions.generateDataDump(null, null, function(error, successMessage) {
  console.log("LAMBDA CALLBACK:");
  console.log(error);
  console.log(successMessage);
});