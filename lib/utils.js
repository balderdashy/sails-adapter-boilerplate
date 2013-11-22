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
    if(attr.autoIncrement) attr.type = 'serial';

    var str = [
      '"' + key + '"',                        // attribute name
      utils.sqlTypeCast(attr.type),           // attribute type
      attr.primaryKey ? 'PRIMARY KEY' : '',   // primary key
      attr.unique ? 'UNIQUE' : ''             // unique constraint
    ].join(' ').trim();

    schema += str + ', ';
  });

  // Remove trailing seperator/trim
  return schema.slice(0, -2);
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

utils.normalizeSchema = function(schema) {
  var normalized = {};
  var clone = _.clone(schema);

  clone.forEach(function(column) {

    // Set type
    normalized[column.Column] = { type: column.Type };

    // Check for primary key
    if (column.Constraint && column.C === 'p') {
      normalized[column.Column].primaryKey = true;
    }
  });
};