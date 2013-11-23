var tests = require('waterline-adapter-tests'),
    adapter = require('../../lib/adapter'),
    mocha = require('mocha');

/**
 * SQLite3 configuration
 */

var config = {
  filename: ":memory:",
  mode: sqlite3.OPEN_READWRITE | OPEN_CREATE,
  verbose: true
};

/**
 * Run Tests
 */

var suite = new tests({ adapter: adapter, config: config });