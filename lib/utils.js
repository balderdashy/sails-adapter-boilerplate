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
* Safe hasOwnProperty
*/
utils.object = {};
/**
* Safer helper for hasOwnProperty checks
*
* @param {Object} obj
* @param {String} prop
* @return {Boolean}
* @api public
*/
var hop = Object.prototype.hasOwnProperty;
	utils.object.hasOwnProperty = function(obj, prop) {
	return hop.call(obj, prop);
};

/**
* Escape Name
*
* Wraps a name in quotes to allow reserved
* words as table or column names such as user.
*/
function escapeName(name) {
	return '"' + name + '"';
}
utils.escapeName = escapeName;

/**
 * Builds a Select statement determining if Aggeragate options are needed.
 */

utils.buildSelectStatement = function(criteria, table, attributes, schema) {
  var query = '';

  // Escape table name
  var schemaName = criteria._schemaName ? utils.escapeName(criteria._schemaName) + '.' : '';
  var tableName = schemaName + utils.escapeName(table);

  if (criteria.groupBy || criteria.sum || criteria.average || criteria.min ||
    criteria.max) {

    query = 'SELECT rowid, ';

    // Append groupBy columns to select statement
    if(criteria.groupBy) {
      if(criteria.groupBy instanceof Array) {
        criteria.groupBy.forEach(function(opt){
          query += tableName + '.' + utils.escapeName(opt) + ', ';
        });

      } else {
        query += tableName + '.' + utils.escapeName(criteria.groupBy) + ', ';
      }
    }

    // Handle SUM
    if (criteria.sum) {
      if(criteria.sum instanceof Array) {
        criteria.sum.forEach(function(opt){
          query += 'CAST(SUM(' + tableName + '.' + utils.escapeName(opt) +
            ') AS float) AS ' + opt + ', ';
        });

      } else {
        query += 'CAST(SUM(' + tableName + '.' +
          utils.escapeName(criteria.sum) + ') AS float) AS ' + criteria.sum +
          ', ';
      }
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      if(criteria.average instanceof Array) {
        criteria.average.forEach(function(opt){
          query += 'CAST(AVG(' + tableName + '.' + utils.escapeName(opt) +
            ') AS float) AS ' + opt + ', ';
        });

      } else {
        query += 'CAST(AVG(' + tableName + '.' +
          utils.escapeName(criteria.average) + ') AS float) AS ' +
          criteria.average + ', ';
      }
    }

    // Handle MAX
    if (criteria.max) {
      if(criteria.max instanceof Array) {
        criteria.max.forEach(function(opt){
          query += 'MAX(' + tableName + '.' + utils.escapeName(opt) + ') AS ' +
            opt + ', ';
        });

      } else {
        query += 'MAX(' + tableName + '.' + utils.escapeName(criteria.max) +
          ') AS ' + criteria.max + ', ';
      }
    }

    // Handle MIN
    if (criteria.min) {
      if(criteria.min instanceof Array) {
        criteria.min.forEach(function(opt){
          query += 'MIN(' + tableName + '.' + utils.escapeName(opt) + ') AS ' +
            opt + ', ';
        });

      } else {
        query += 'MIN(' + tableName + '.' + utils.escapeName(criteria.min) + ') AS ' +
          criteria.min + ', ';
      }
    }

    // trim trailing comma
    query = query.slice(0, -2) + ' ';

    // Add FROM clause
    return query += 'FROM ' + table + ' ';
  }


  query += 'SELECT rowid, ';

  var selectKeys = [], joinSelectKeys = [];
  Object.keys(schema[table]).forEach(function(key) {    
    selectKeys.push({ table: table, key: key });
  });

  // Check for joins
  if (criteria.joins) {

    var joins = criteria.joins;

    joins.forEach(function(join) {
      if (!join.select) return;

      Object.keys(
        schema[join.child.toLowerCase()].schema
      ).forEach(function(key) {
        var _join = _.cloneDeep(join);
        _join.key = key;
        joinSelectKeys.push(_join);
      });

      // Remove the foreign key for this join from the selectKeys array
      selectKeys = selectKeys.filter(function(select) {
        var keep = true;
        if (select.key === join.parentKey && join.removeParentKey) keep = false;
        return keep;
      });
    });
  }

  // Add all the columns to be selected that are not joins
  selectKeys.forEach(function(select) {
    query += utils.escapeName(select.table) + '.' + utils.escapeName(select.key) + ', ';
  });

  // Add all the columns from the joined tables
  joinSelectKeys.forEach(function(select) {

    // Create an alias by prepending the child table with the alias of the join
    var alias = select.alias.toLowerCase() + '_' + select.child.toLowerCase();

    // If this is a belongs_to relationship, keep the foreign key name from the
    // AS part of the query. This will result in a selected column like:
    // "user"."id" AS "user_id__id"
    if (select.model) {
      return query += utils.escapeName(alias) + '.' +
        utils.escapeName(select.key) + ' AS ' +
        utils.escapeName(select.parentKey + '__' + select.key) + ', ';
    }

    // If a junctionTable is used, the child value should be used in the AS part
    // of the select query.
    if (select.junctionTable) {
      return query += utils.escapeName(alias) + '.' +
        utils.escapeName(select.key) + ' AS ' +
        utils.escapeName(select.alias + '_' + select.child + '__' + select.key)
        + ', ';
    }

    // Else if a hasMany attribute is being selected, use the alias plus the
    // child.
    return query += utils.escapeName(alias) + '.' + utils.escapeName(select.key)
      + ' AS ' + utils.escapeName(select.alias + '_' + select.child + '__' +
      select.key) + ', ';
  });

  // Remove the last comma
  query = query.slice(0, -2) + ' FROM ' + tableName + ' ';

  return query;
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
 * Group Results into an Array
 *
 * Groups values returned from an association query into a single result.
 * For each collection association the object returned should have an array
 * under the user defined key with the joined results.
 *
 * @param {Array} results returned from a query
 * @return {Object} a single values object
 */
utils.group = function(values) {

  var self = this;
  var joinKeys = [];
  var _value;

  if (!Array.isArray(values)) return values;

  // Grab all the keys needed to be grouped
  var associationKeys = [];

  values.forEach(function(value) {
    Object.keys(value).forEach(function(key) {
      key = key.split('__');
      if (key.length === 2) associationKeys.push(key[0].toLowerCase());
    });
  });

  associationKeys = _.unique(associationKeys);

  // Store the values to be grouped by id
  var groupings = {};

  values.forEach(function(value) {

    // Add to groupings
    if (!groupings[value.id]) groupings[value.id] = {};

    associationKeys.forEach(function(key) {
      if(!Array.isArray(groupings[value.id][key]))
        groupings[value.id][key] = [];
      var props = {};

      Object.keys(value).forEach(function(prop) {
        var attr = prop.split('__');
        if (attr.length === 2 && attr[0] === key) {
          props[attr[1]] = value[prop];
          delete value[prop];
        }
      });

      // Don't add empty records that come from a left join
      var empty = true;

      Object.keys(props).forEach(function(prop) {
        if (props[prop] !== null) empty = false;
      });

      if (!empty) groupings[value.id][key].push(props);
    });
  });

  var _values = [];

  values.forEach(function(value) {
    var unique = true;

    _values.forEach(function(_value) {
      if (_value.id === value.id) unique = false;
    });

    if (!unique) return;

    Object.keys(groupings[value.id]).forEach(function(key) {
      value[key] = _.uniq(groupings[value.id][key], 'id');
    });

    _values.push(value);
  });

  return _values;
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
