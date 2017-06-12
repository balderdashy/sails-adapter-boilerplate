/*---------------------------------------------------------------
  :: sails-sqlite3
  -> adapter

  The code here is loosely based on sails-postgres adapter.
---------------------------------------------------------------*/

// Dependencies
var sqlite3 = require('sqlite3'),
    async = require('async'),
    fs = require('fs'),
    _ = require('lodash'),
    Query = require('./query'),
    utils = require('./utils');
    Errors = require('waterline-errors').adapter

module.exports = (function() {

  var connections = {};
  var dbs = {};

  // Determines whether the database file already exists
  var exists = false;

  var adapter = {
    identity: 'sails-sqlite3',

    // Set to true if this adapter supports (or requires) things like data
    // types, validations, keys, etc.  If true, the schema for models using this
    // adapter will be automatically synced when the server starts.
    // Not terribly relevant if not using a non-SQL / non-schema-ed data store
    syncable: true,

    // Default configuration for collections
    // (same effect as if these properties were included at the top level of the
    // model definitions)
    defaults: {

      // Valid values are filenames, ":memory:" for an anonymous in-memory database and an empty string for an
      // anonymous disk-based database. Anonymous databases are not persisted and when closing the database handle,
      // their contents are lost.
      filename: "",
      mode: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
      verbose: false
    },

    /*************************************************************************/
    /* Public Methods for Sails/Waterline Adapter Compatibility              */
    /*************************************************************************/

    /**
     * This method runs when a model is initially registered
     * at server-start-time.  This is the only required method.
     *
     * @param  {[type]}   connection [description]
     * @param  {[type]}   collection [description]
     * @param  {Function} cb         [description]
     * @return {[type]}              [description]
     */
    registerConnection: function(connection, collections, cb) {
    	//console.log("registering connection for " + connection.identity);
    	//console.log(cb.toString())
      var self = this;

      if (!connection.identity) return cb(Errors.IdentityMissing);
      if (connections[connection.identity]) return cb(Errors.IdentityDuplicate);

      connections[connection.identity] = {
        config: connection,
        collections: collections
      };

      async.map(Object.keys(collections), function(columnName, cb) {
        self.describe(connection.identity, columnName, cb);
      }, cb);
    },


    /**
     * Fired when a model is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {[type]}   connectionId [description]
     * @param  {Function} cb           [description]
     * @return {[type]}                [description]
     */
    teardown: function(connectionId, cb) {
      if (!connections[connectionId]) return cb();
      //console.log("Tearing down connection " + connectionId)
      delete connections[connectionId];
      cb();
    },

    /**
     * This method returns attributes and is required when integrating with a
     * schemaful database.
     *
     * @param  {[type]}     connectionId [description]
     * @param  {[type]}     collection   [description]
     * @param  {[Function]} cb           [description]
     * @return {[type]}                  [description]
     */
    describe: function(connectionId, table, cb) {
      var self = this;

      spawnConnection(connectionId, function __DESCRIBE__(client, cb) {

        var connection = connections[connectionId];
        var collection = connection.collections[table];
               
        // Get a list of all the tables in this database
        // See: http://www.sqlite.org/faq.html#q7)
        var query = 'SELECT * FROM sqlite_master WHERE type="table" AND name="' + table + '" ORDER BY name';
        
        client.get(query, function (err, schema) {
          if (err || !schema) return cb();
          //console.log("client.get")
          //console.log(schema);
          // Query to get information about each table
          // See: http://www.sqlite.org/pragma.html#pragma_table_info
          var columnsQuery = "PRAGMA table_info(" + schema.name + ")";

          // Query to get a list of indices for a given table
          var indexListQuery = 'PRAGMA index_list("' + schema.name + '")';

          schema.indices = [];
          schema.columns = [];

          var index = { columns: [] };

          client.each(indexListQuery, function(err, currentIndex) {
        	  //console.log(currentIndex)
            // Query to get information about indices
            var indexInfoQuery =
              'PRAGMA index_info("' + currentIndex.name + '")';

            // Retrieve detailed information for given index
            client.each(indexInfoQuery, function(err, indexedCol) {
              index.columns.push(indexedCol);
            });

            schema.indices.push(currentIndex);
          }, function(err, resultCount) {
            if (err) return cb(err);

            client.each(columnsQuery, function(err, column) {

              // In SQLite3, AUTOINCREMENT only applies to PK columns of
              // INTEGER type
              column.autoIncrement = (column.type.toLowerCase() == 'integer'
                && column.pk == 1);

              // By default, assume column is not indexed until we find that it
              // is
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

              // This callback function is fired when all the columns have been
              // iterated by the .each() function
              if (err) {
                console.error("Error while retrieving column information.");
                console.error(err);
                return cb(err);
              }
              //console.log("schema")
              //console.log(schema)
              var normalizedSchema = utils.normalizeSchema(schema);
              //console.log(normalizedSchema);
              try {
            	// Set internal schema mapping
                  collection.schema = normalizedSchema;
            	  
              } catch(e){
            	  console.log(e);
            	  //console.log(connection.collections[table]);
            	  //console.log(table)
            	  //console.log(connection)
              }
              

              // Fire the callback with the normalized schema
              cb(null, normalizedSchema);
            });
          });
        });
      }, cb);
    },


    /**
     * Creates a new table in the database when defining a model.
     *
     * @param  {[type]}     connectionId [description]
     * @param  {[type]}     table        [description]
     * @param  {[type]}     definition   [description]
     * @param  {[Function]} cb           [description]
     * @return {[type]}                  [description]
     */
    define: function(connectionId, table, definition, cb) {

      var describe = function(err, result) {
        if (err) return cb(err);

        adapter.describe(connectionId, table.replace(/["']/g, ""), cb);
      };

      spawnConnection(connectionId, function __DEFINE__(client, cb) {

        // Escape table name
        table = utils.escapeTable(table);

        // Iterate through each attribute, building a query string
        var _schema = utils.buildSchema(definition);

        // Check for any index attributes
        var indices = utils.buildIndexes(definition);

        // Build query
        var query = 'CREATE TABLE ' + table + ' (' + _schema + ')';
        //client.on("trace", console.log)
        //client.on("profile", console.log)
        //console.log(client.run);
        client.run(query, function(err) {
          if (err) return cb(err);

          // Build indices
          function buildIndex(name, cb) {

            // Strip slashes from tablename, used to namespace index
            var cleanTable = table.replace(/['"]/g, '');

            // Build a query to create a namespaced index tableName_key
            var query = 'CREATE INDEX ' + cleanTable + '_' + name + ' on ' +
              table + ' (' + name + ');';

            // Run query
            client.run(query, function(err) {
              if (err) return cb(err);
              cb();
            });
          }
          async.eachSeries(indices, buildIndex, cb);
        });
      }, describe);
    },


    /**
     * Drops a table corresponding to the model.
     *
     * @param  {[type]}     connectionId [description]
     * @param  {[type]}     table        [description]
     * @param  {[type]}     relations    [description]
     * @param  {[Function]} cb           [description]
     * @return {[type]}                  [description]
     */
    drop: function(connectionId, table, relations, cb) {

      if (typeof relations == 'function') {
        cb = relations;
        relations = [];
      }

      spawnConnection(connectionId, function __DROP__(client, cb) {

        function dropTable(item, next) {

          // Build query
          var query = 'DROP TABLE ' + utils.escapeTable(table);

          // Run the query
          client.run(query, function(err) {
            cb(null, null);
          });
        }

        async.eachSeries(relations, dropTable, function(err) {
          if (err) return cb(err);
          dropTable(table, cb);
        });
      }, cb);
    },


    /**
     * Add a column to the table.
     *
     * @param  {[type]}     connectionId [description]
     * @param  {[type]}     table        [description]
     * @param  {[type]}     attrName     [description]
     * @param  {[type]}     attrDef      [description]
     * @param  {[Function]} cb           [description]
     * @return {[type]}                  [description]
     */
    addAttribute: function(connectionId, table, attrName, attrDef, cb) {
      spawnConnection(connectionId, function __ADD_ATTRIBUTE__(client, cb) {

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
      }, cb);
    },


    /**
     * Remove attribute from table.
     * In SQLite3, this is tricky since there's no support for DROP COLUMN
     * in ALTER TABLE. We'll have to rename the old table, create a new table
     * with the same name minus the column and copy all the data over.
     */
    removeAttribute: function(connectionId, table, attrName, cb) {
      spawnConnection(connectionId, function __REMOVE_ATTRIBUTE__(client, cb) {

        // NOTE on this method just so I don't forget: Below is a pretty hackish
        // way to remove attributes. Proper SQLite way would be to write all of
        // the logic below into a single SQL statement wrapped in BEGIN TRANSAC-
        // TION and COMMIT block like this:
        //
        // BEGIN TRANSACTION;
        // ALTER TABLE table RENAME TO table_old_;
        // CREATE TABLE table(attrName1, ...);
        // INSERT INTO table SELECT attrName1, ... FROM table_old_;
        // DROP TABLE table_old_;
        // COMMIT;
        //
        // This will ensure that removing attribute would be atomic. For now,
        // hacking it cause I'm actually feeling lazy.

        var oldTable = table + '_old_';

        // Build query to rename table
        var renameQuery = 'ALTER TABLE ' + utils.escapeTable(table) +
          ' RENAME TO ' + utils.escapeTable(oldTable);

        // Run query
        client.run(query, function(err) {
          if (err) return cb(err);

          // Get the attributes
          adapter.describe(connectionId, oldTable, function(err, schema) {
            if (err) return cb(err);

            // Deep copy the schema and remove the attribute
            var newAttributes = _.clone(schema);
            delete newAttributes[attrName];

            // Recreate the table
            adapter.define(connectionId, table, newAttributes,
              function (err, schema) {
                if (err) return cb(err);

                // Copy data back from old table to new table
                var copyQuery = 'INSERT INTO ' + utils.escapeTable(table) +
                  ' SELECT rowid, ';

                Object.keys(newAttributes).forEach(
                  function(colName, idx, columns) {
                    copyQuery += colName;
                    if (idx < keys.length)
                      copyQuery += ', '
                  }
                );

                copyQuery += ' FROM ' + utils.escapeTable(oldTable);

                client.run(copyQuery, function(err) {
                  if (err) return cb(err);

                  var dropQuery = 'DROP TABLE ' + utils.escapeTable(oldTable);

                  client.run(dropQuery, function(err) {
                    if (err) return cb(err);

                    // End of operation!
                    cb();
                  });
                });
              }
            );
          });
        });
      }, cb);
    },


    /**
     * Finds and returns an instance of a model that matches search criteria.
     */
    // REQUIRED method if users expect to call Model.find(), Model.findAll() or related methods
    // You're actually supporting find(), findAll(), and other methods here
    // but the core will take care of supporting all the different usages.
    // (e.g. if this is a find(), not a findAll(), it will only send back a single model)
    find: function(connectionId, table, options, cb) {
      spawnConnection(connectionId, function __FIND__(client, cb) {

        // Check if this is an aggregate query and that there's something to return
        if (options.groupBy || options.sum || options.average || options.min ||
          options.max) {
          if (!options.sum && !options.average && !options.min &&
            !options.max) {
            return cb(Errors.InvalidGroupBy);
          }
        }

        var connection = connections[connectionId];
        var collection = connection.collections[table];

        // Grab connection schema
        var schema = getSchema(connection.collections);
        
        // Build query
        var queryObj = new Query(collection.definition, schema);
        var query = queryObj.find(table, options);

        // Cast special values
        var values = [];

        // Run query
        client.each(query.query, query.values, function(err, row) {
          if (err) return cb(err);

          values.push(queryObj.cast(row));
        }, function(err, resultCount) {
          var _values = options.joins ? utils.group(values) : values;

          cb(null, values);
        });
      }, cb);
    },


    /**
     * Add a new row to the table
     */
    // REQUIRED method if users expect to call Model.create() or any methods
    create: function(connectionId, table, data, cb) {
        spawnConnection(connectionId, function __CREATE__(client, cb) {

            //  Grab Connection Schema
    	    var connection = connections[connectionId];
    	    var collection = connection.collections[table];
    	    // 	Grab connection schema
    	    var schema = getSchema(connection.collections);
     
    	    // 	Build query
    		var _query = new Query(collection.schema, schema);

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
          }, cb);
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


    // REQUIRED method if users expect to call Model.update()
    update: function(connectionId, table, options, data, cb) {
      spawnConnection(connectionId, function __UPDATE__(client, cb) {

        //  Grab Connection Schema
	    var connection = connections[connectionId];
	    var collection = connection.collections[table];
	    // 	Grab connection schema
	    var schema = getSchema(connection.collections);
 
	    // 	Build query
		var _query = new Query(collection.schema, schema);	 

        // Build a query for the specific query strategy
        var selectQuery = _query.find(table, options); 
        var updateQuery = _query.update(table, options, data);
        var primaryKeys = [];        

        client.serialize(function() {
        	
          // Run query
          client.run(updateQuery.query, updateQuery.values, function(err) {
            if (err) { console.error(err); return cb(err); }

            // Build a query to return updated rows
            if (this.changes > 0) {

        	  adapter.find(connectionId, table.replace(/["']/g, ""), options, function(err, models) {
            	  if (err) { console.error(err); return cb(err); }
            	  //console.log(arguments)
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
      }, cb);
    },

    // REQUIRED method if users expect to call Model.destroy()
    destroy: function(connectionId, table, options, cb) {
    	
      spawnConnection(connectionId, function __DELETE__(client, cb) {

    	  var connection = connections[connectionId];
    	  var collection = connection.collections[table];
    	  //console.log("definition: "  + JSON.stringify(collection.definition))
    	  var _schema = utils.buildSchema(collection.definition);	
    	      	  
    	  // Build a query for the specific query strategy
    	  var _query = new Query(_schema);
    	  var query = _query.destroy(table, options);
    	  
    	  // Run Query
    	  adapter.find(connectionId, table.replace(/["']/g, ""), options, function(err, models) {
    		  //if (err) { console.log(err); return cb(err); }
    		  
    		  //console.log("adapter.find")
    		  //console.log(arguments)
    		  var values = [];
    		  models.forEach(function(model) {
    			  values.push(_query.cast(model));
    		  });
    		  
	    	  client.run(query.query, query.values, function __DELETE__(err, result) {
	    		  //console.log(arguments)
		    	  if(err) return cb(handleQueryError(err));
		    	  cb(null, values);
	    	  });
    	  });
    	  
      }, cb);
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

  /***************************************************************************/
  /* Private Methods
  /***************************************************************************/

  function getSchema(collections){
	  var schema = {};
	  Object.keys(collections).forEach(function(collectionId) {
      	schema[collectionId] = collections[collectionId].schema;
      });
	  return schema; 
  }
  
  function spawnConnection(connectionName, logic, cb) {
	  //console.log("spawnConnection")
	  //console.log("connectionName " + connectionName)
	  //console.log(connections)
    var connectionObject = connections[connectionName];
    if (!connectionObject) return cb(Errors.InvalidConnection);

    var connectionConfig = connectionObject.config;

    // Check if we want to run in verbose mode
    // Note that once you go verbose, you can't go back.
    // See: https://github.com/mapbox/node-sqlite3/wiki/API
    if (connectionConfig.verbose) sqlite3 = sqlite3.verbose();
    
    // Make note whether the database already exists
    exists = fs.existsSync(connectionConfig.filename);

    // Create a new handle to our database
    var client = new sqlite3.Database(
      connectionConfig.filename,
      connectionConfig.mode,
      function(err) {
        after(err, client);
      }
    );

    function after(err, client) {
      if (err) {
        console.error("Error creating/opening SQLite3 database: " + err);

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
