var assert = require('assert'),
	adapter = require('../lib/adapter.js');

describe('Creating Nodes', function () {
	var nodes = [];

 	before(function(done) {
        var connection = {
			identity: 'neo4j'
		};
		adapter.registerConnection(connection,null,done);
    });

	it('should create one node with a property test = "1"', function (done) {
		adapter.create("neo4j", null, { test: 1 }, function(err, results) {
			if (err) { throw err; }
			nodes.push(results);
			done();
		});
	});

	it('should create multiple nodes with the property test = "1"', function(done) {
		adapter.createMany("neo4j", null,{params:[{ test: 1 },{ test: 1 }]}, function(err, results) {
			if (err) { throw err; }
			nodes.push(results);
			done();
		});
	});
});
