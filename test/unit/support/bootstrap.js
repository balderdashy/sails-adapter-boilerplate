var sqlite3 = require('sqlite3'),
    adapter = require('../../lib/adapter');

var Support = module.exports = {};

Support.Config = {
  filename: 'sailssqlite.db',
  mode: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
  verbose: true
};

Support.Definition = {
  field_1: { type: 'string' },
  field_2: { type: 'string' },
  id: {
    type: 'integer',
    autoIncrement: true,
    defaultsTo: 'AUTO_INCREMENT',
    primaryKey: true
  }
};

Support.Collection = function(name) {
  return {
    identity: name,
    config: Support.Config,
    definition: Support.Definition
  };
};

// Register and define a collection
Support.Setup = function(tableName, cb) {
  adapter.registerCollection(Support.Collection(tableName), function(err) {
    if (err) return cb(err);
    adapter.define(tableName, Support.Definition, cb);
  });
};

// Remove a table
Support.Teardown = function(tableName, cb) {
  var client = new sqlite3.Database(Support.Config.filename, Support.Config.mode, function(err) {
    dropTable(tableName, client, function(err) {
      if (err) {
        done();
        return cb(err);
      }

      done();
      return cb();
    });
  });
};

function dropTable(table, client, cb) {
  table = '"' + table + '"';

  var query = "DROP TABLE " + table;
  client.run(query, cb);
}