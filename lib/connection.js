var neo4j = require('neo4j-js'),
    Q = require('q');

module.exports = (function() {
  var graph = false, d = Q.defer(); // if an active connection exists, use it instead of tearing the previous one down
  
  function connect(connection) {
    if (!graph) {
      var path = connection.protocol + connection.host + ':' + connection.port + connection.base;
      console.log('Connecting to Neo4j@' + path);
      neo4j.connect(path, function(err, graphObj) {
        if (err) {
          console.log('An error has occured when trying to connect to Neo4j:');
          d.reject(err);
          throw err;
        }
        console.log('Connected@Neo4j');
        graph = graphObj;
        d.resolve(graph);
        return d.promise;
      });
    }
    else {
      return d.promise;
    }
  }

  function graphDo(cb) {
    d.promise.then(cb);
  }

  // built in this pattern so this can be enhanced later on
  return {
    connect: connect,
    graph: graphDo
  };
})();