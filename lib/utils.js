const _ = require('@sailshq/lodash');

const utils = module.exports = {};

// CONSTANT Regular Expressions for data types
const coreAffinities = /TEXT|INTEGER|REAL|BLOB|NUMERIC/i;
const intTypes = /INT/i;
const textTypes = /CHAR|CLOB/i;
const realTypes = /DOUBLE|FLOAT/i;

/**
 * Build a schema from an attributes object
 * @return Object {declaration: string, schema: object}
 */
utils.buildSchema = function(obj, foreignKeys) {
  let schema = {};
  let columnDefs = [];
  let constraintDefs = [];

  // Iterate through the Object Keys and build a string
  Object.keys(obj).forEach(function(key) {
    const attr = obj[key];

    const sqliteType = utils.sqlTypeCast(attr.columnType, key);
    const affinity = utils.getAffinity(sqliteType);

    schema[key] = {
      primaryKey: attr.primaryKey,
      unique: attr.unique,
      indexed: attr.unique || attr.primaryKey, // indexing rules in sqlite
      type: affinity
    }

    // Note: we are ignoring autoincrement b/c it is only supported on
    // primary key defs, but probably shouldn't be used there anyway
    // https://sqlite.org/autoinc.html
    const def = [
      '"' + key + '"',                        // attribute name
      sqliteType,                             // attribute type
      attr.primaryKey ? 'PRIMARY KEY' : '',   // primary key
      attr.unique ? 'UNIQUE' : ''             // unique constraint
    ].join(' ').trim();

    columnDefs.push(def);
  });

  for (let columnName in foreignKeys) {
    const keyDef = foreignKeys[columnName];
    constraintDefs.push(`FOREIGN KEY(${columnName}) REFERENCES ${keyDef.table}(${keyDef.column})`);
  }

  return {
    declaration: columnDefs.concat(constraintDefs).join(', '),
    schema
  };
};

/**
 * Sqlite3 defines types as "affinities" since all columns can contain
 * all types
 */
utils.getAffinity = sqliteType => {
  if (!sqliteType) return 'BLOB'; // essentially no type

  const matches = coreAffinities.exec(sqliteType);
  if (matches !== null) return matches[0].toUpperCase();

  if (intTypes.exec(sqliteType) !== null) return 'INTEGER';
  if (textTypes.exec(sqliteType) !== null) return 'TEXT';
  if (realTypes.exec(sqliteType) !== null) return 'REAL';

  return 'NUMERIC';
};

/**
 * @return Map by unescaped tablename of the foreign key fields and the tables
 * they look up to
 */
utils.buildForeignKeyMap = physicalModelsReport => {
  const foreignKeyMap = {};

  for (let tableName in physicalModelsReport) {
    tableKeys = {};
    foreignKeyMap[tableName] = tableKeys;

    for (let columnName in physicalModelsReport[tableName].definition) {
      const column = physicalModelsReport[tableName].definition[columnName];

      if (column.foreignKey) {
        tableKeys[column.columnName] = {
          table: column.references,
          column: column.on
        }
      }
    }
  }

  return foreignKeyMap;
}

/**
* Escape Name
*
* Wraps a name in quotes to allow reserved
* words as table or column names such as user.
*/
utils.escapeName = utils.escapeTable = name => `"${name}"`;

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
 * Map data from a stage-3 query to its representation
 * in Sqlite. Collects a list of columns and processes
 * an group of records.
 * @param recordList An array of records to be processed
 * @param schema The schema for the table
 * @return Object with below properties:
 *  - keys: array of keys (column names) in the order the data will be represented
 *  - values: array of values. Values are ordered such that they line up
 *            exactly with a flattened version of the paramLists property
 *  - paramLists: array of arrays. Each element of the outer array is an array of
 *              values in the same order as keys.
 */
utils.mapAllAttributes = function(recordList, schema) {
  const keys = new Map();
  const valueMaps = [];

  for (let record of recordList) {
    const recordValueMap = {}
    valueMaps.push(recordValueMap);

    for (let columnName in record) {
      keys.set(columnName, `"${columnName}"`);
      recordValueMap[columnName] =
        utils.prepareValue(record[columnName], schema[columnName].type);
    }
  }

  const keyList = [];
  const objKeys = [];

  // create set order of columns (keys)
  for (let entry of keys) {
    objKeys.push(entry[0]);
    keyList.push(entry[1]);
  }

  const paramLists = [];
  let i = 1;
  const valueList = [];
  for (let values of valueMaps) {
    const paramList = [];
    paramLists.push(paramList);

    for (let key of objKeys) {
      let nextValue = values[key];

      if (nextValue === undefined || nextValue === null) {
        valueList.push(null);
      } else {
        valueList.push(nextValue);
      }

      paramList.push('$' + i);
      i++;
    }
  }

  return ({ keys: keyList, values: valueList, paramLists: paramLists });
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
    normalized[column.name].indexed = !!column.indexed;

    // Show unique constraint
    normalized[column.name].unique = !!column.unique;

    normalized[column.name].autoIncrement = !!column.autoIncrement;
  });

  return normalized;
};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function(value, columnType) {
  if (value === null) return null;

  // Cast dates to SQL
  if (_.isDate(value)) {
    switch(columnType) {
      case 'TEXT':
        value = value.toUTCString();
        break;
      case 'INTEGER':
      case 'REAL':
        value = value.valueOf();
      default:
        throw new Error(`Cannot cast date to ${columnType}`);
    }

    return value;
  }

  // Cast functions to strings
  if (_.isFunction(value)) {
    if (columnType !== 'TEXT') throw new Error('Function can only cast to TEXT');
    return value.toString();
  }

  // Check buffers for BLOB typs
  if (Buffer.isBuffer(value)) {
    if (columnType !== 'BLOB') throw new Error('Buffers may only represent BLOB types');
  }

  // Store Arrays / Objects as JSON strings
  if (typeof value === 'object' && columnType !== 'BLOB') {
    return JSON.stringify(value);
  }

  switch(columnType) {
    case 'TEXT':
      return value.toString();
    case 'INTEGER':
      if (typeof value === 'boolean') return value ? 1 : 0;
      return parseInt(value, 0);
    case 'REAL':
      return parseFloat(value);
  }

  return value; //BLOB or NUMERIC
};

/**
 * Cast waterline types to SQLite3 data types
 */
utils.sqlTypeCast = function(type, columnName) {
  // type has been explicitly specified by the user
  if (!type.startsWith('_')) return type;

  switch (type.toLowerCase()) {
    case '_string':
      return 'TEXT';

    case '_boolean':
      return `INTEGER CHECK(${columnName} IN (0, 1))`;

    case '_numberkey':
    case '_numbertimestamp': //dates
      return 'INTEGER';

    case '_number':
      return 'REAL';

    case '_ref':
      return 'BLOB';

    case '_json':
      return 'TEXT';

    default:
      console.error("Warning: Unregistered type given: " + type);
      return 'TEXT';
  }
};
