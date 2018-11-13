require('dotenv').load()
var lambdaFunctions = require('./lambda')
lambdaFunctions.importAlbums(null, null, function(error, successMessage) {
  console.log("LAMBDA CALLBACK:");
  console.log(error);
  console.log(successMessage);
});