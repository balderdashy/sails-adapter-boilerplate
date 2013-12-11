var assert = require('assert');

describe('init', function () {

	it('should succeed when a valid connection is created', function (done) {
		var adapter = require('../lib/adapter.js');
		adapter.getConnection().then(function() {
			assert.equal(adapter.getConnection().inspect().state, 'fulfilled');
			done();
		});
	});
});