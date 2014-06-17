var assert = require('assert');

describe('Creating Nodes', function () {
	var nodes = [];
	it('should create one node with a property test = "1"', function (done) {
		var adapter = require('../lib/adapter.js');
		adapter.createConnection();
		adapter.create(null, { test: 1 }, function(err, results) {
			if (err) { throw err; }
			nodes.push(results);
			done();
		});
	});

	it('should create multiple nodes with the property test = "1"', function(done) {
		var adapter = require('../lib/adapter.js');
		adapter.createConnection();
		adapter.createMany(null,{params:[{ test: 1 },{ test: 1 }]}, function(err, results) {
			if (err) { throw err; }
			nodes.push(results);
			done();
		});
	});
});
