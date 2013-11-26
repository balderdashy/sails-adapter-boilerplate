var tests = require('waterline-adapter-tests'),
    sqlite3 = require('sqlite3'),
    adapter = require('../../lib/adapter'),
    mocha = require('mocha');

/**
 * SQLite3 configuration
 */

var config = {
  filename: "sailssqlite.db",
  mode: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
  verbose: true
};

/**
 * Run Tests
 */

var suite = new tests({ adapter: adapter, config: config });