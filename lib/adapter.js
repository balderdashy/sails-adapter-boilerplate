/*---------------------------------------------------------------
  :: sails-neo4j
  -> adapter
---------------------------------------------------------------*/

var async = require('async'),
    neo = require('./connection'),
    security = require('./helpers/security'),
    _ = require('lodash');

var adapter = module.exports = (function() {

  // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
  // If true, the schema for models using this adapter will be automatically synced when the server starts.
  // Not terribly relevant if not using a non-SQL / non-schema-ed data store
  var syncable = false,
      connection,

  // Including a commitLog config enables transactions in this adapter
  // Please note that these are not ACID-compliant transactions: 
  // They guarantee *ISOLATION*, and use a configurable persistent store, so they are *DURABLE* in the face of server crashes.
  // However there is no scheduled task that rebuild state from a mid-step commit log at server start, so they're not CONSISTENT yet.
  // and there is still lots of work to do as far as making them ATOMIC (they're not undoable right now)
  //
  // However, for the immediate future, they do a great job of preventing race conditions, and are
  // better than a naive solution.  They add the most value in findOrCreate() and createEach().
  // 
  // commitLog: {
  //  identity: '__default_mongo_transaction',
  //  adapter: 'sails-mongo'
  // },

  // Default configuration for collections
  // (same effect as if these properties were included at the top level of the model definitions)
    defaults = {
      // change these to fit your setup
      protocol: 'http://',
      port: 7474,
      host: 'localhost',
      base: '/db/data/',
      debug: false

      // If setting syncable, you should consider the migrate option, 
      // which allows you to set how the sync will be performed.
      // It can be overridden globally in an app (config/adapters.js) and on a per-model basis.
      //
      // drop   => Drop schema and data, then recreate it
      // alter  => Drop/add columns as necessary, but try 
      // safe   => Don't change anything (good for production DBs)
      // migrate: 'alter'
    };

  //Init

  connection = neo.connect(defaults);

  function parseOne(values) {
    var i, names = [], name;
    for (i in values) {
      if (values.hasOwnProperty(i)) {
        name = i + ': {' + i + '}';
        names.push(name);
      }
    }
    return names.join(',');
  }

  function parseMany(values) {
    var i, names = [], name;
    for (i in values) {
      if (values.hasOwnProperty(i)) {
        if (Object.prototype.toString.call(values[i]) === '[object Array]') {
          name = i;
        }
        else {
          return false;
        }
        names.push(name);
      }
    }
    return names.join(',');
  }

  function andJoin(object, properties, andKeyword, namespace) {
    var query = [], q;
    for (var i in properties) {
      if (properties.hasOwnProperty(i)) {
        var equality = '=';
        if (properties[i].hasOwnProperty('=~'))
        {
          properties[i] = '(?i)' + properties[i]['='];
          equality = '=~';
        }
        var field = object + '.' + i;
        if (i==='id')
        {
          field = 'id('+object+')';
          properties[i] = parseInt(properties[i]);
        }
        completeName = (typeof namespace === 'undefined' || namespace === null) ? i : namespace + '_' + i; 
        q = field + equality + '{' + completeName + '}';
        query.push(q);
      }
    }
    return query.join(andKeyword);
  }

  function toWhere(object, params, namespace) {
    properties = params.where;
    if (!properties)
      return '';
    var query = [], q;
    var count = 0;
    for (var i in properties) {
      count++;
    }
    if (count === 1 && properties.hasOwnProperty('or'))
    {
      var targetProperties = {};
      for (var i in properties['or'])
      {
        q = '(' + andJoin(object, properties['or'][i], 'AND', namespace) + ')';
        query.push(q);
        _.extend(targetProperties,properties['or'][i]);
      }
      params.where = targetProperties;
    }
    else
      query.push('(' + andJoin(object, properties, 'AND', namespace) + ')');

    return '(' + query.join(' OR ') + ')';
  }

  function query(q, params, cb, unique) {
    neo.graph(function(gr) {
      gr.query(q.join('\n'), params, function (err, results) {
        if (err) {
          cb(err, null);
        }
        else {
          for(i=0; i<results.length;i++)
          {
            data = _.clone(results[i].data);
            id = _.pick(results[i], 'id');
            id.id = parseInt(id.id);
            results[i] = _.extend(id, data);
          }
          if (unique) cb(null, results[0]);
          else cb(null, results);
        }
      });
    });
  }

  function getConnection() {
    return neo.connect(defaults);
  }

  return {
    syncable: syncable,
    defaults: defaults,
    getConnection: getConnection,
    sanitized: security.sanitized,
    query: query,

    create: function(collectionName, params, cb) {

      var q, delimiter = '';
      
      if (collectionName === null) { collectionName = ''; } // do we have a label?
      else { collectionName = ':' + collectionName; }
      
      if (params !== null && collectionName !== null) { delimiter = ' AND '; } // do we have a label and params?

      q = [
        'CREATE (n' + collectionName + ' { ' + parseOne(params) + ' })',
        'RETURN n'
      ];

      query(q, params, cb, true);
    },

    createMany: function(collectionName, params, cb) {

      var q, delimiter = '';
      
      if (collectionName === null) { collectionName = ''; } // do we have a label?
      else { collectionName = ':' + collectionName; }
      
      if (params && collectionName) { delimiter = ' AND '; } // do we have a label and params?
      
      q = [
        'CREATE (n' + collectionName + ' { ' + parseMany(params) + ' })',
        'RETURN n'
      ];

      query(q, params, cb, false);
    },

    find: function(collectionName, params, cb) {

      var q, delimiter = '';
      
      if (collectionName === null) { collectionName = ''; } // do we have a label?
      else { collectionName = 'n:' + collectionName; }

      if (params.where && collectionName) { delimiter = ' AND '; } // do we have a label and params?
      q = [
        'MATCH (n)',
        'WHERE ' + collectionName + delimiter + toWhere('n', params),
        'RETURN n'
      ];

      query(q, params.where, cb, false);
    },

    update: function(collectionName, params, values, cb) {

      var q, delimiter = '';
      
      if (collectionName === null) { collectionName = ''; } // do we have a label?
      else { collectionName = 'n:' + collectionName; }

      if (params.where && collectionName) { delimiter = ' AND '; } // do we have a label and params?
      q = [
        'MATCH (n)',
        'WHERE ' + collectionName + delimiter + toWhere('n', params),
        'SET ' + andJoin('n', _.omit(values, 'id'), ','),
        'RETURN n'
      ];

      params.where = _.extend(params.where, values);
      
      query(q, params.where, cb, true);
    },

    destroy: function(collectionName, params, cb) {
      var q, delimiter = '';
      
      if (collectionName === null) { collectionName = ''; } // do we have a label?
      else { collectionName = 'n:' + collectionName; }

      if (params.where && collectionName) { delimiter = ' AND '; } // do we have a label and params?
      q = [
        'MATCH (n)',
        'WHERE ' + collectionName + delimiter + toWhere('n', params),
        'DELETE n'
      ];
      query(q, params.where, cb, true);
    },



    // REQUIRED method if users expect to call Model.stream()
    stream: function(collectionName, options, stream) {
      // options is a standard criteria/options object (like in find)

      // stream.write() and stream.end() should be called.
      // for an example, check out:
      // https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247

    },

    link: function(collectionName, predecessorParams, successorCollectionName, successorParams, relationshipType, relationshipParams, cb) {
      var q, predecessorDelimiter = '', successorDelimiter = '', predecessorNamespace = 'pred', successorNamespace = 'succ';

      if (collectionName === null) { collectionName = ''; } // do we have a label?
      else { collectionName = 'a:' + collectionName; }
      if (predecessorParams && collectionName) { predecessorDelimiter = ' AND '; }
      
      if (successorCollectionName === null) { successorCollectionName = ''; } // do we have a label?
      else { successorCollectionName = 'b:' + successorCollectionName; }
      if (successorParams && collectionName) { successorDelimiter = ' AND '; }

      relationshipParams = _.isEmpty(relationshipParams) ? '' : ' ' + JSON.stringify(relationshipParams);

      q = [
        'MATCH (a),(b)',
        'WHERE ' + collectionName + predecessorDelimiter + toWhere('a', {where: predecessorParams}, predecessorNamespace) 
        +' AND ' + successorCollectionName + successorDelimiter + toWhere('b', {where: successorParams}, successorNamespace),
        'CREATE (a)-[n:' + relationshipType + relationshipParams + ']->(b)',
        'RETURN n'
      ];
      
      params = {};
      _.each(predecessorParams, function(value, key) {
        key = predecessorNamespace + '_' + key;
        params[key] = value;
      });
      _.each(successorParams, function(value, key) {
        key = successorNamespace + '_' + key;
        params[key] = value;
      });

      query(q, params, cb, true);
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

})();
