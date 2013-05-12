describe('registerCollection', function () {

	it('should not hang or encounter any errors', function (cb) {
		var adapter = require('../index.js');
		adapter.registerCollection(cb);
	});

	// e.g.
	// it('should create a mysql connection pool', function () {})
	// it('should create an HTTP connection pool', function () {})
	// ... and so on.
});