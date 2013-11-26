var _ = require("underscore");

var utils = module.exports = {};

/**
 * Build a schema from an attributes object
 */

utils.buildSchema = function(obj) {
  var schema = "";
  
  // Iterate through the Object Keys and build a string
  Object.keys(obj).forEach(function(key) {
    var attr = {};

    // Normalize Simple Key/Value attribute
    // ex: name: 'string'
    if(typeof obj[key] === 'string') {
      attr.type = obj[key];
    }

    // Set the attribute values to the object key
    else {
      attr = obj[key];
    }

    // Override Type for autoIncrement
    if(attr.autoIncrement) {
      attr.type = 'serial';
      attr.primaryKey = true;
    }

    var str = [
      '"' + key + '"',                        // attribute name
      utils.sqlTypeCast(attr.type),           // attribute type
      attr.primaryKey ? 'PRIMARY KEY' : '',   // primary key
      attr.unique ? 'UNIQUE' : '',             // unique constraint
      attr.defaultsTo ? 'DEFAULT "' + attr.defaultsTo + '"': ''
    ].join(' ').trim();

    schema += str + ', ';
  });

  // Remove trailing seperator/trim
  return schema.slice(0, -2);
};


/**
 * Builds a Select statement determining if Aggeragate options are needed.
 */

utils.buildSelectStatement = function(criteria, table) {
  var query = '';

  if (criteria.groupBy || criteria.sum || criteria.average || criteria.min || criteria.max) {
    query = 'SELECT rowid, ';

    // Append groupBy columns to select statement
    if(criteria.groupBy) {
      if(criteria.groupBy instanceof Array) {
        criteria.groupBy.forEach(function(opt){
          query += opt + ', ';
        });

      } else {
        query += criteria.groupBy + ', ';
      }
    }

    // Handle SUM
    if (criteria.sum) {
      if(criteria.sum instanceof Array) {
        criteria.sum.forEach(function(opt){
          query += 'CAST(SUM(' + opt + ') AS float) AS ' + opt + ', ';
        });

      } else {
        query += 'CAST(SUM(' + criteria.sum + ') AS float) AS ' + criteria.sum + ', ';
      }
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      if(criteria.average instanceof Array) {
        criteria.average.forEach(function(opt){
          query += 'CAST(AVG(' + opt + ') AS float) AS ' + opt + ', ';
        });

      } else {
        query += 'CAST(AVG(' + criteria.average + ') AS float) AS ' + criteria.average + ', ';
      }
    }

    // Handle MAX
    if (criteria.max) {
      if(criteria.max instanceof Array) {
        criteria.max.forEach(function(opt){
          query += 'MAX(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MAX(' + criteria.max + ') AS ' + criteria.max + ', ';
      }
    }

    // Handle MIN
    if (criteria.min) {
      if(criteria.min instanceof Array) {
        criteria.min.forEach(function(opt){
          query += 'MIN(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MIN(' + criteria.min + ') AS ' + criteria.min + ', ';
      }
    }

    // trim trailing comma
    query = query.slice(0, -2) + ' ';

    // Add FROM clause
    return query += 'FROM ' + table + ' ';
  }

  // Else select ALL
  return 'SELECT rowid, * FROM ' + table + ' ';
};


/**
 * Build an Index array from any attributes that
 * have an index key set.
 */

utils.buildIndexes = function(obj) {
  var indexes = [];

  // Iterate through the Object keys and pull out any index attributes
  Object.keys(obj).forEach(function(key) {
    if (obj[key].hasOwnProperty('index')) indexes.push(key);
  });
  
  return indexes;
};


/**
 * Escape Table Name
 *
 * Wraps a table name in quotes to allow reserved
 * words as table names such as user.
 */

utils.escapeTable = function(table) {
  return '"' + table + '"';
};

utils.mapAttributes = function(data) {
  var keys = [],   // Column Names
      values = [], // Column Values
      params = [], // Param Index, ex: $1, $2
      i = 1;

  Object.keys(data).forEach(function(key) {
    keys.push('"' + key + '"');
    values.push(utils.prepareValue(data[key]));
    params.push('$' + i);
    i++;
  });

  return({ keys: keys, values: values, params: params });
};

utils.normalizeSchema = function(schema) {
  var normalized = {};
  var clone = _.clone(schema);

  clone.columns.forEach(function(column) {

    // Set type
    normalized[column.name] = { type: column.type };

    // Check for primary key
    normalized[column.name].primaryKey = column.pk ? true : false;

    // Indicate whether the column is indexed
    normalized[column.name].indexed = column.indexed;

    // Show unique constraint
    if (column.unique) normalized[column.name].unique = true;

    if (column.autoIncrement) normalized[column.name].autoIncrement = true;
  });

  return normalized;
};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function(value) {

  // Cast dates to SQL
  if (_.isDate(value)) {
    value = utils.toSqlDate(value);
  }

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  // Store Arrays as strings
  if (Array.isArray(value)) {
    value = JSON.stringify(value);
  }

  // Store Buffers as hex strings (for BYTEA)
  if (Buffer.isBuffer(value)) {
    value = '\\x' + value.toString('hex');
  }

  return value;
};

/**
 * Cast waterline types to SQLite3 data types
 */

utils.sqlTypeCast = function(type) {
  switch (type.toLowerCase()) {
    case 'serial':
      return 'INTEGER';

    case 'string':
    case 'text':
      return 'TEXT';

    case 'boolean':
    case 'int':
    case 'integer':
      return 'INTEGER';

    case 'float':
    case 'double':
      return 'REAL';

    case 'date':
    case 'datestamp':
    case 'datetime':
      return 'TEXT';

    case 'array':
      return 'TEXT';

    case 'json':
      return 'TEXT';

    case 'binary':
    case 'bytea':
      return 'BLOB';

    default:
      console.error("Warning: Unregistered type given: " + type);
      return 'TEXT';
  }
};

/**
 * JS Date to UTC Timestamp
 *
 * Dates should be stored in Postgres with UTC timestamps
 * and then converted to local time on the client.
 */

utils.toSqlDate = function(date) {
  return date.toUTCString();
};