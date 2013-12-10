/*global neoAdapter */

var neo4j = require('neo4j-js').
    neoAdapter = require('./adapter.js');


module.exports = (function() {
  var activeConnection = false; // if an active connection exists, use it instead of tearing the previous one down
  
  function connect(cb) {
    var path = neoAdapter.defaults.host + ':' + neoAdapter.defaults.port + neoAdapter.defaults.base;
    if (!activeConnection) {
      neo4j.connect(path, function(err, graph) {
        if (err) {
          cb(err, null);
        }
        else {
          activeConnection = graph;
          return cb(false, activeConnection);
        }
      });
    }
    else {
      return cb(false, activeConnection);
    }
  }

  // API
  return {
    connect: connect,

  };
})();