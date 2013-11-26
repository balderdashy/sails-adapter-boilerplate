/*---------------------------------------------------------------
  :: sails-sqlite3
  -> adapter

  The code here is loosely based on sails-postgres adapter.
---------------------------------------------------------------*/

// Dependencies
var sqlite3 = require('sqlite3'),
    async = require('async'),
    fs = require('fs'),
    _ = require("underscore"),
    Query = require('./query'),
    utils = require('./utils');

module.exports = (function() {

  var dbs = {};

  // Determines whether the database file already exists
  var exists = false;

  var adapter = {
    identity: 'sails-sqlite3',

    // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
    // If true, the schema for models using this adapter will be automatically synced when the server starts.
    // Not terribly relevant if not using a non-SQL / non-schema-ed data store
    syncable: false,

    // Default configuration for collections
    // (same effect as if these properties were included at the top level of the model definitions)
    defaults: {

      // Valid values are filenames, ":memory:" for an anonymous in-memory database and an empty string for an
      // anonymous disk-based database. Anonymous databases are not persisted and when closing the database handle,
      // their contents are lost.
      filename: "",

      mode: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
      verbose: false
    },


    // This method runs when a model is initially registered at server start time
    registerCollection: function(collection, cb) {
      var def = _.clone(collection);
      var key = def.identity;
      var definition = def.definition || {};

      if (dbs[key]) return cb();
      dbs[key.toString()] = def;

      // Set default primary key as 'id'
      var pkName = "id";

      // Set primary key field
      for (var attribute in definition) {
        if (!definition[attribute].hasOwnProperty('primaryKey')) continue;

        // Check if custom primaryKey value is false
        if (!definition[attribute].primaryKey) continue;

        // Set the pkName to the custom primaryKey value
        pkName = attribute;
      }

      // Set the primary key to the definition object
      def.primaryKey = pkName;

      // Always call describe
      this.describe(key, function(err, schema) {
        if (err) return cb(err);
        cb(null, schema);
      });
    },


    // The following methods are optional
    ////////////////////////////////////////////////////////////

    // Optional hook fired when a model is unregistered, typically at server halt
    // useful for tearing down remaining open connections, etc.
    teardown: function(cb) {
      cb();
    },


    // Raw query interface
    query: function(table, query, data, cb) {
      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }

      spawnConnection(function __QUERY__(client, cb) {
        if (data) client.all(query, data, cb);
        client.all(query, cb);
      }, dbs[table].config, cb);
    },

    // REQUIRED method if integrating with a schemaful database
    define: function(table, definition, cb) {
      
      var describe = function(err, result) {
        if (err) return cb(err);

        adapter.describe(table.replace(/["']/g, ""), cb);
      };

      spawnConnection(function __DEFINE__(client, cb) {

        // Escape table name
        table = utils.escapeTable(table);

        // Iterate through each attribute, building a query string
        var _schema = utils.buildSchema(definition);

        // Check for any index attributes
        var indices = utils.buildIndexes(definition);

        // Build query
        var query = 'CREATE TABLE ' + table + ' (' + _schema + ')';

        // Run the query
        client.run(query, function(err) {
          if (err) return cb(err);

          // Build indices
          function buildIndex(name, cb) {

            // Strip slashes from tablename, used to namespace index
            var cleanTable = table.replace(/['"]/g, '');

            // Build a query to create a namespaced index tableName_key
            var query = 'CREATE INDEX ' + cleanTable + '_' + name + ' on ' + table + ' (' + name + ');';

            // Run query
            client.run(query, function(err) {
              if (err) return cb(err);
              cb();
            });
          }
          async.eachSeries(indices, buildIndex, cb);
        });
      }, dbs[table].config, describe);
    },

    // REQUIRED method if integrating with a schemaful database
    describe: function(table, cb) {
      var self = this;

      spawnConnection(function __DESCRIBE__(client, cb) {

        // Get a list of all the tables in this database (see http://www.sqlite.org/faq.html#q7)
        var query = 'SELECT * FROM sqlite_master WHERE type="table" AND name="' + table + '" ORDER BY name';

        client.get(query, function (err, schema) {
          if (err || !schema) return cb();

          // Query to get information about each table (see http://www.sqlite.org/pragma.html#pragma_table_info)
          var columnsQuery = "PRAGMA table_info(" + schema.name + ")";

          // Query to get a list of indices for a given table
          var indexListQuery = 'PRAGMA index_list("' + schema.name + '")';

          schema.indices = [];
          schema.columns = [];

          var index = { columns: [] };

          client.each(indexListQuery, function(err, currentIndex) {

            // Query to get information about indices
            var indexInfoQuery = 'PRAGMA index_info("' + currentIndex.name + '")';

            // Retrieve detailed information for given index
            client.each(indexInfoQuery, function(err, indexedCol) {
              index.columns.push(indexedCol);
            });

            schema.indices.push(currentIndex);
          }, function(err, resultCount) {
            if (err) return cb(err);

            client.each(columnsQuery, function(err, column) {

              // In SQLite3, AUTOINCREMENT only applies to PK columns of INTEGER type
              column.autoIncrement = column.type.toLowerCase() == 'integer' && column.pk == 1;

              // By default, assume column is not indexed until we find that it is
              column.indexed = false;

              // Search for indexed columns
              schema.indices.forEach(function(idx) {
                if (!column.indexed) {
                  index.columns.forEach(function(indexedCol) {
                    if (indexedCol.name == column.name) {
                      column.indexed = true;
                      if (idx.unique) column.unique = true;
                    }
                  });
                }
              });

              schema.columns.push(column);
            }, function(err, resultCount) {
              
              // This callback function is fired when all the columns have been iterated by the .each() function
              if (err) {
                console.error("Error while retrieving column information.");
                console.error(err);
                return cb(err);
              }

              var normalizedSchema = utils.normalizeSchema(schema);

              // Set internal schema mapping
              dbs[table].schema = normalizedSchema;

              // Fire the callback with the normalized schema
              cb(null, normalizedSchema);
            });
          });
        });
      }, dbs[table].config, cb);
    },

    // REQUIRED method if integrating with a schemaful database
    drop: function(table, cb) {
      spawnConnection(function __DROP__(client, cb) {

        // Escape table name
        table = utils.escapeTable(table);

        // Build query
        var query = 'DROP TABLE ' + table;

        // Run the query
        client.run(query, function(err) {
          cb(null, null);
        });
      }, dbs[table].config, cb);
    },

    // Optional override of built-in alter logic
    // Can be simulated with describe(), define(), and drop(),
    // but will probably be made much more efficient by an override here
    // alter: function (collectionName, attributes, cb) { 
    // Modify the schema of a table or collection in the data store
    // cb(); 
    // },


    // Add a column to the table
    addAttribute: function(table, attrName, attrDef, cb) {
      spawnConnection(function __ADD_ATTRIBUTE__(client, cb) {
        
        // Escape table name
        table = utils.escapeTable(table);

        // Set up a schema definition
        var attrs = {};
        attrs[attrName] = attrDef;

        var _schema = utils.buildSchema(attrs);

        // Build query
        var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + _schema;

        // Run query
        client.run(query, function(err) {
          if (err) return cb(err);
          cb(null, this);
        });
      }, dbs[table].config, cb);
    },


    // Remove attribute from table
    // In SQLite3, this is tricky since there's no support for DROP COLUMN 
    // in ALTER TABLE. We'll have to rename the old table, create a new table
    // with the same name minus the column and copy all the data over.
    removeAttribute: function(table, attrName, cb) {
      spawnConnection(function __REMOVE_ATTRIBUTE__(client, cb) {

        // Escape table name
        table = utils.escapeTable(table);

        // Build query to rename table
        var renameQuery = 'ALTER TABLE ' + table + ' RENAME TO ' + table + '_old_';

      }, dbs[table].config, cb);
    },


    // REQUIRED method if users expect to call Model.create() or any methods
    create: function(table, data, cb) {
      spawnConnection(function __CREATE__(client, cb) {

        // Build a query object
        var _query = new Query(dbs[table].definition);

        // Escape table name
        table = utils.escapeTable(table);

        // Transform the data object into arrays used in parametrized query
        var attributes = utils.mapAttributes(data),
            columnNames = attributes.keys.join(', '),
            paramValues = attributes.params.join(', ');

        // Build query
        var insertQuery = 'INSERT INTO ' + table + ' (' + columnNames + ') values (' + paramValues + ')';
        var selectQuery = 'SELECT * FROM ' + table + ' ORDER BY rowid DESC LIMIT 1';

        // First insert the values
        client.run(insertQuery, attributes.values, function(err) {
          if (err) return cb(err);

          // Get the last inserted row
          client.get(selectQuery, function(err, row) {
            if (err) return cb(err);

            var values = _query.cast(row);

            cb(null, values);
          });
        });
      }, dbs[table].config, cb);
    },

    // REQUIRED method if users expect to call Model.find(), Model.findAll() or related methods
    // You're actually supporting find(), findAll(), and other methods here
    // but the core will take care of supporting all the different usages.
    // (e.g. if this is a find(), not a findAll(), it will only send back a single model)
    find: function(table, options, cb) {
      spawnConnection(function __FIND__(client, cb) {

        // Check if this is an aggregate query and that there's something to return
        if (options.groupBy || options.sum || options.average || options.min || options.max) {
          if (!options.sum && !options.average && !options.min && !options.max) {
            return cb(new Error('Cannot perform groupBy without a calculation'));
          }
        }

        // Build a query object
        var _query = new Query(dbs[table].definition);

        // Escape table name
        table = utils.escapeTable(table);

        // Build query
        var _schema = dbs[table.replace(/["']/g, "")].schema;
        var query = new Query(_schema).find(table, options);

        // Cast special values
        var values = [];

        // Run query
        client.each(query.query, query.values, function(err, row) {
          if (err) return cb(err);

          values.push(_query.cast(row));
        }, function(err, resultCount) {
          cb(null, values);
        });
      }, dbs[table].config, cb);
    },

    // REQUIRED method if users expect to call Model.update()
    update: function(table, options, data, cb) {
      spawnConnection(function __UPDATE__(client, cb) {

        // Build a query object
        var _query = new Query(dbs[table].definition);

        // Escape table name
        table = utils.escapeTable(table);

        // Build query
        var primaryKey = dbs[table.replace(/["']/g, "")].primaryKey;
        var _schema = dbs[table.replace(/["']/g, "")].schema;
        var selectQuery = 'SELECT "' + primaryKey + '" FROM ' + table; //new Query(_schema).find(table, options);
        var updateQuery = new Query(_schema).update(table, options, data);
        var primaryKeys = [];

        var criteria = _query._build(options);

        selectQuery += ' ' + criteria.query;

        client.serialize(function() {

          // Keep track of the row IDs of the rows that will be updated
          client.each(selectQuery, criteria.values, function(err, row) {
            if (err) return cb(err);
            primaryKeys.push(row[primaryKey]);
          });

          // Run query
          client.run(updateQuery.query, updateQuery.values, function(err) {
            if (err) { console.error(err); return cb(err); }

            // Build a query to return updated rows
            if (this.changes > 0) {

              // Build criteria
              var criteria;
              if (this.changes == 1) {
                criteria = { where: {}, limit: 1 };
                criteria.where[primaryKey] = primaryKeys[0];
              } else {
                criteria = { where: {} };
                criteria.where[primaryKey] = primaryKeys;
              }

              // Return the updated items up the callback chain
              adapter.find(table.replace(/["']/g, ""), criteria, function(err, models) {
                if (err) { console.log(models); return cb(err); }

                var values = [];

                models.forEach(function(item) {
                  values.push(_query.cast(item));
                });

                cb(null, values);
              });
            } else {
              console.error('WARNING: No rows updated.');
              cb(null);
            }
          });
        });
      }, dbs[table].config, cb);
    },

    // REQUIRED method if users expect to call Model.destroy()
    destroy: function(table, options, cb) {
      spawnConnection(function __DELETE__(client, cb) {

        // Build a query object
        var _query = new Query(dbs[table].definition);

        // Escape table name
        table = utils.escapeTable(table);

        // Build query
        var _schema = dbs[table.replace(/["']/g, "")].schema;
        var deleteQuery = new Query(_schema).destroy(table, options);

        // Run query
        adapter.find(table.replace(/["']/g, ""), options, function(err, models) {
          if (err) { console.log(err); return cb(err); }

          var values = [];

          models.forEach(function(model) {
            values.push(_query.cast(model));
          });

          client.run(deleteQuery.query, deleteQuery.values, function(err) {
            if (err) return cb(err);
            cb(null, values);
          });
        });
      }, dbs[table].config, cb);
    },



    // REQUIRED method if users expect to call Model.stream()
    stream: function(table, options, stream) {
      // options is a standard criteria/options object (like in find)

      // stream.write() and stream.end() should be called.
      // for an example, check out:
      // https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247
      if (dbs[table].config.verbose) sqlite3 = sqlite3.verbose();

      var client = new sqlite3.Database(dbs[table].config.filename, dbs[table].config.mode, function(err) {
        if (err) return cb(err);

        // Escape table name
        table = utils.escapeTable(table);

        // Build query
        var query = new Query(dbs[table.replace(/["']/g, "")].schema).find(table, options);

        // Run the query
        client.each(query.query, query.values, function(err, row) {
          if (err) {
            stream.end();
            client.close();
          }

          stream.write(row);
        }, function(err, resultCount) {
          stream.end();
          client.close();
        });
      });
    }



    /*
    **********************************************
    * Optional overrides
    **********************************************

    // Optional override of built-in batch create logic for increased efficiency
    // otherwise, uses create()
    createEach: function (collectionName, cb) { cb(); },

    // Optional override of built-in findOrCreate logic for increased efficiency
    // otherwise, uses find() and create()
    findOrCreate: function (collectionName, cb) { cb(); },

    // Optional override of built-in batch findOrCreate logic for increased efficiency
    // otherwise, uses findOrCreate()
    findOrCreateEach: function (collectionName, cb) { cb(); }
    */


    /*
    **********************************************
    * Custom methods
    **********************************************

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // > NOTE:  There are a few gotchas here you should be aware of.
    //
    //    + The collectionName argument is always prepended as the first argument.
    //      This is so you can know which model is requesting the adapter.
    //
    //    + All adapter functions are asynchronous, even the completely custom ones,
    //      and they must always include a callback as the final argument.
    //      The first argument of callbacks is always an error object.
    //      For some core methods, Sails.js will add support for .done()/promise usage.
    //
    //    + 
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////


    // Any other methods you include will be available on your models
    foo: function (collectionName, cb) {
      cb(null,"ok");
    },
    bar: function (collectionName, baz, watson, cb) {
      cb("Failure!");
    }


    // Example success usage:

    Model.foo(function (err, result) {
      if (err) console.error(err);
      else console.log(result);

      // outputs: ok
    })

    // Example error usage:

    Model.bar(235, {test: 'yes'}, function (err, result){
      if (err) console.error(err);
      else console.log(result);

      // outputs: Failure!
    })

    */
  };

  //////////////                 //////////////////////////////////////////
  ////////////// Private Methods //////////////////////////////////////////
  //////////////                 //////////////////////////////////////////
  function spawnConnection(logic, config, cb) {
    // Check if we want to run in verbose mode
    // Note that once you go verbose, you can't go back (see https://github.com/mapbox/node-sqlite3/wiki/API)
    if (config.verbose) sqlite3 = sqlite3.verbose();

    // Make note whether the database already exists
    exists = fs.existsSync(config.filename);

    // Create a new handle to our database
    var client = new sqlite3.Database(config.filename, config.mode, function(err) {
      after(err, client);
    });
    
    function after(err, client) {
      if (err) {
        console.error("Error creating/opening SQLite3 database.");
        console.error(err);

        // Close the db instance on error
        if (client) client.close();

        return cb(err);
      }

      // Run the logic
      logic(client, function(err, result) {
        // Close db instance after it's done running
        client.close();

        return cb(err, result);
      });
    }
  }

  return adapter;
})();