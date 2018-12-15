/**
 * Dependencies
 */

var _ = require('@sailshq/lodash'),
    utils = require('./utils'),
    hop = utils.object.hasOwnProperty;

/**
 * Query Builder for creating parameterized queries for use
 * with the SQLite3 adapter
 *
 * Most of this code was adapted from the Query class of
 * Postgres adapter
 *
 * If you have any questions, contact Andrew Jo <andrewjo@gmail.com>
 */

const Query = function (tableName, schema, model) {
  this._values = [];
  this._paramCount = 1;
  this._query = '';
  this._tableName = tableName;
  /** Waterline model - provides info on type */
  this._model = model || {};

  this._schema = _.clone(schema);

  return this;
};

/**
 * SELECT Statement
 */

Query.prototype.find = function(criteria) {

  this._query = utils.buildSelectStatement(criteria, this._tableName, this._schema);

  if (criteria) this._build(criteria);

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * UPDATE Statement
 */

Query.prototype.update = function(criteria, data) {
  this._query = 'UPDATE ' + utils.escapeTable(this._tableName) + ' ';

  // Transform the Data object into arrays used in a parameterized query
  var attributes = utils.mapAllAttributes([data], this._schema);

  const params = attributes.paramLists[0];

  // Update the paramCount
  this._paramCount = params.length + 1;

  // Build SET string
  const assignments = [];
  for (var i = 0; i < attributes.keys.length; i++) {
    assignments.push(`${attributes.keys[i]} = ${params[i]}`);
  }

  this._query += `SET ${assignments.join(', ')} `;

  // Add data values to this._values
  this._values = attributes.values;
  // Build criteria clause
  if (criteria) this._build({ where: criteria.where });

  return {
    query: this._query,
    values: this._values
  };
};


/**
 * DELETE Statement
 */

Query.prototype.destroy = function(criteria) {
  this._query = `DELETE FROM ${utils.escapeTable(this._tableName)} `;
  if (criteria) this._build(criteria);

  return {
    query: this._query,
    values: this._values
  };
};


/**
 * String Builder
 */

Query.prototype._build = function(criteria) {

  // Evaluate criteria in correct order
  if (criteria.where) this.where(criteria.where);
  if (criteria.sort) this.sort(criteria.sort);
  if (criteria.limit) this.limit(criteria.limit);
  if (criteria.skip) this.skip(criteria.skip);

  return {
    query: this._query,
    values: this._values
  }
};

/**
 * Specifiy a `where` condition
 *
 * `Where` conditions may use key/value model attributes for simple query
 * look ups. Complex conditions are grouped in 'AND' and 'OR' arrays
 *
 * The following conditions are supported along with simple criteria:
 *
 *   Conditions:
 *     [And, Or]
 *
 *   Criteria Operators:
 *     [<, <=, >, >=, !=, nin, in, like]
 *
 * ####Example
 *
 *   where: {
 *    and: [
 *      {name: 'foo'},
 *      {age: { '>': 25 }},
 *      {desc: {like: '%hello%'}}
 *    ]
 *   }
 */
Query.prototype.where = function(criteria) {
  if (Object.keys(criteria).length > 0) {
    const criteriaTree = new Criterion(this._schema);
    criteriaTree.addCriterion(criteria);

    const parsedCriteria = criteriaTree.generateCriteria();
    this._query += parsedCriteria.whereClause;
    this._values = this._values.concat(parsedCriteria.values);
  }
}

/**
 * Utility class for building criteria. Constructs a tree and recurses down
 * it to build the final string
 */
class Criterion {
  constructor(schema, joinString = '', rawCriterion){
    this._schema = schema;
    this._joinString = joinString;
    this._isLeaf = !!rawCriterion;
    this._criteriaList = [];
    this._rawCriterion = rawCriterion
    this._paramIndex = 1;
  }

  addCriterion(rawCriterion) {
    const newCriterion = this._constructCriterionTree(rawCriterion);

    this._criteriaList.push(newCriterion);
  }

  /**
   * Private method
   * @param {Object} rawCriterion
   */
  _constructCriterionTree(rawCriterion) {
    let subCriterion;
    if (!!rawCriterion.and) {
      subCriterion = new Criterion(this._schema, ' AND ');

      for (let andElement of rawCriterion.and) {
        subCriterion.addCriterion(andElement);
      }
    } else if (!!rawCriterion.or) {
      subCriterion = new Criterion(this._schema, ' OR ');

      for (let orElement of rawCriterion.or) {
        subCriterion.addCriterion(orElement);
      }
    } else {
      subCriterion = new Criterion(this._schema, '', rawCriterion);
    }

    return subCriterion;
  }

  /**
   * Private Method. Converts list to string.
   * Increments _paramIndex
   * @param {Array} list
   * @return {Object}
   *        @param {String} setString
   *        @param {Array} dedupedValues
   */
  _arrayToSqlSet(list) {
    const valueSet = new Set(list);
    const dedupedValues = [];
    const paramList = [];

    for (let val of valueSet) {
      dedupedValues.push(val);
      paramList.push(`$${this._paramIndex++}`);
    }

    return {
      setString: `(${paramList.map(obj => obj.toString()).join(', ')})`,
      dedupedValues
    };
  }

  /**
   * Performs the logic of generating the criteria. If not a leaf, recurses
   * down and wraps sub-criteria in parentheses
   * @return {Object}
   *        @property {String} whereClause The constructed string
   *        @property {Array<Object>} values The values matching params
   *        @property {Number} paramIndex The number of parameters generated by this function
   */
  generateCriteria(paramIndex = -1) {
    //differentiate from root call
    if (paramIndex > 0) {
      this._paramIndex = paramIndex;
    }

    if (!this._isLeaf) {
      const subCriteriaResults = this._criteriaList
        .map(criterion => {
          const result = criterion.generateCriteria(this._paramIndex);
          this._paramIndex = result.paramIndex;

          return result;
        });

      const values = subCriteriaResults.reduce((previous, next) => {
        return previous.concat(next.values);
      }, []);

      const whereClause = (paramIndex === -1 ? 'WHERE ' : '')
        + subCriteriaResults
          .map(sub => `(${sub.whereClause})`)
          .join(this._joinString);

      return {whereClause, values, paramIndex: this._paramIndex};
    }

    /**** Leaf Node Code *****/
    if (Object.keys(this._rawCriterion).length !== 1) {
      throw new Error('Unexpected query object: exactly one column key expected');
    }
    let columnName = Object.keys(this._rawCriterion)[0];
    let condition = this._rawCriterion[columnName];
    let values = [];
    let whereClause;
    // get schema / model type so we know how to process
    const columnType = this._schema[columnName].type;

    if (condition == null) {
      whereClause = `${columnName} IS NULL`;
    } else if (typeof condition != 'object' || columnType === 'BLOB') {
      whereClause = `${columnName} = $${this._paramIndex++}`;
      values.push(condition);
    } else {
      const conditionKeys = Object.keys(condition);
      if (conditionKeys.length !== 1) {
        throw new Error('Multiple conditions detected for one column condition. The adapter is confused');
      }

      const operator = conditionKeys[0];
      let dedupedValues, setString;
      switch (operator) {
        case '>':
        case '>=':
        case '<':
        case '<=':
        case 'like':
          whereClause = `${columnName} ${operator.toUpperCase()} $${this._paramIndex++}`;
          values.push(condition[operator]);
          break;
        case '!=':
          whereClause = `${columnName} <> $${this._paramIndex++}`;
          values.push(condition[operator]);
          break;
        case 'in':
          ({dedupedValues, setString} = this._arrayToSqlSet(condition[operator]))
          values = dedupedValues;
          whereClause = `${columnName} IN ${setString}`;
          break;
        case 'nin':
          ({ dedupedValues, setString } = this._arrayToSqlSet(condition[operator]))
          values = dedupedValues;
          whereClause = `${columnName} IN ${setString}`;
          break;
      }
    }

    return {whereClause, values, paramIndex: this._paramIndex};
  }
}

Query.prototype.whereBak = function(options) {
  var self = this,
      operators = this.operators();

  if (!options || Object.keys(options).length === 0) return;

  // Begin WHERE query
  this._query += 'WHERE ';

  // Process 'where' criteria
  Object.keys(options).forEach(function(key) {

    switch (key.toLowerCase()) {
      case 'or':
        options[key].forEach(function(statement) {
          Object.keys(statement).forEach(function(key) {

            switch (key) {
              case 'and':
                Object.keys(statement[key]).forEach(function(attribute) {
                  operators.and(attribute, statement[key][attribute], ' OR ');
                });
                return;

              case 'like':
                Object.keys(statement[key]).forEach(function(attribute) {
                  operators.like(attribute, key, statement, ' OR ');
                });
                return;

              default:
                if(typeof statement[key] === 'object') {
                  Object.keys(statement[key]).forEach(function(attribute) {
                    operators.and(attribute, statement[key][attribute], ' OR ');
                  });
                  return;
                }

                operators.and(key, statement[key], ' OR ');
                return;
            }
          });
        });

        return;

      case 'like':
        Object.keys(options[key]).forEach(function(parent) {
          operators.like(parent, key, options);
        });

        return;

      // Key/Value
      default:

        // 'IN'
        if (options[key] instanceof Array) {
          operators.in(key, options[key]);
          return;
        }

        // 'AND'
        operators.and(key, options[key]);
        return;
    }
  });

  // Remove trailing AND if it exists
  if (this._query.slice(-4) === 'AND ') {
    this._query = this._query.slice(0, -5);
  }

  // Remove trailing OR if it exists
  if (this._query.slice(-3) === 'OR ') {
    this._query = this._query.slice(0, -4);
  }
};

/**
 * Operator Functions
 */

Query.prototype.operators = function() {
  var self = this;

  var sql = {
    and: function(key, options, comparator) {
      var caseSensitive = true;

      // Check if key is a string
      if (self._schema[key] && self._schema[key].type === 'TEXT') caseSensitive = false;

      processCriteria.call(self, key, options, '=', caseSensitive);
      self._query += (comparator || ' AND ');
    },

    like: function(parent, key, options, comparator) {
      var caseSensitive = true;

      // Check if parent is a string
      if (self._schema[parent].type === 'TEXT') caseSensitive = false;

      processCriteria.call(self, parent, options[key][parent], 'ILIKE', caseSensitive);
      self._query += (comparator || ' AND ');
    },

    in: function(key, options) {
      var caseSensitive = true;

      // Check if key is a string
      if (self._schema[key].type === 'TEXT') caseSensitive = false;

      // Check case sensitivity to decide if LOWER logic is used
      if (!caseSensitive) key = 'LOWER("' + key + '")';
      else key = '"' + key + '"'; // for case sensitive camelCase columns

      // Build IN query
      self._query += key + ' IN (';

      // Append each value to query
      options.forEach(function(value) {
        self._query += '$' + self._paramCount + ', ';
        self._paramCount++;

        // If case sensitivity is off, lowercase the value
        if (!caseSensitive) value = value.toLowerCase();

        self._values.push(value);
      });

      // Strip last comma and close criteria
      self._query = self._query.slice(0, -2) + ')';
      self._query += ' AND ';
    }
  };

  return sql;
};

/**
 * Process Criteria
 *
 * Processes a query criteria object
 */

function processCriteria(parent, value, combinator, caseSensitive) {
  var self = this;

  // Complex object attributes
  if (typeof value === 'object' && value !== null) {
    var keys = Object.keys(value);

    // Escape parent
    parent = '"' + parent + '"';

    for (var i = 0; i < keys.length; i++) {

      // Check if value is a string and if so add LOWER logic
      // to work with case insensitive queries
      if (!caseSensitive && typeof value[[keys][i]] === 'string') {
        parent = 'LOWER(' + parent + ')';
        value[keys][i] = value[keys][i].toLowerCase();
      }

      self._query += parent + ' ';
      prepareCriterion.call(self, keys[i], value[keys[i]]);

      if (i+1 < keys.length) self._query += ' AND ';
    }

    return;
  }

  // Check if value is a string and if so add LOWER logic
  // to work with case insensitive queries
  if (!caseSensitive && typeof value === 'string') {

    // Escape parent
    parent = '"' + parent + '"';

    // ADD LOWER to parent
    parent = 'LOWER(' + parent + ')';
    value = value.toLowerCase();
  } else {
    // Escape parent
    parent = '"' + parent + '"';
  }

  if (value !== null) {
    // Simple Key/Value attributes
    this._query += parent + ' ' + combinator + ' $' + this._paramCount;

    this._values.push(value);
    this._paramCount++;
  } else {
    this._query += parent + ' IS NULL';
  }
}

/**
 * Prepare Criterion
 *
 * Processes comparators in a query.
 */

function prepareCriterion(key, value) {
  var str;

  switch (key) {
    case '<':
    case 'lessThan':
      this._values.push(value);
      str = '< $' + this._paramCount;
      break;

    case '<=':
    case 'lessThanOrEqual':
      this._values.push(value);
      str = '<= $' + this._paramCount;
      break;

    case '>':
    case 'greaterThan':
      this._values.push(value);
      str = '> $' + this._paramCount;
      break;

    case '>=':
    case 'greaterThanOrEqual':
      this._values.push(value);
      str = '>= $' + this._paramCount;
      break;

    case '!':
    case 'not':
      if (value === null) {
        str = 'IS NOT NULL';
      } else {
        this._values.push(value);
        str = '<> $' + this._paramCount;
      }
      break;

    case 'like':
      this._values.push(value);
      str = 'LIKE $' + this._paramCount;
      break;

    case 'contains':
      this._values.push('%' + value + '%');
      str = 'LIKE $' + this._paramCount;
      break;

    case 'startsWith':
      this._values.push(value + '%');
      str = 'LIKE $' + this._paramCount;
      break;

    case 'endsWith':
      this._values.push('%' + value);
      str = 'LIKE $' + this._paramCount;
      break;

    default:
      throw new Error('Unknown comparator: ' + key);
  }

  // Bump paramCount
  this._paramCount++;

  // Add str to query
  this._query += str;
}

/**
 * Specify a `limit` condition
 */

Query.prototype.limit = function(options) {
  this._query += ' LIMIT ' + options;
};

/**
 * Specify a `skip` condition
 */

Query.prototype.skip = function(options) {
  this._query += ' OFFSET ' + options;
};

/**
 * Specify a `sort` condition
 */

Query.prototype.sort = function(options) {
  if (options.length >>> 0 === 0) return;
  var self = this;

  this._query += ' ORDER BY ';
  const sortItems = [];

  for (let sortItem of options) {
    for (let column in sortItem) {
      sortItems.push(`"${column}" ${sortItem[column]}`);
    }
  }

  this._query += sortItems.join(', ');
};

/**
 * Specify a `group by` condition
 */

Query.prototype.group = function(options) {
  var self = this;

  this._query += ' GROUP BY ';

  // Normalize to array
  if(!Array.isArray(options)) options = [options];

  options.forEach(function(key) {
    self._query += key + ', ';
  });

  // Remove trailing comma
  this._query = this._query.slice(0, -2);
};

/**
 * Cast special values to proper types.
 *
 * Ex: Array is stored as "[0,1,2,3]" and should be cast to proper
 * array for return values.
 */

Query.prototype.castRow = function(values) {
  const newModel = {};

  for (let columnName in values) {
    const attrDef = this._model.definition[columnName];

    if (values[columnName] === null) {
      newModel[columnName] = null;
      continue;
    }

    switch(attrDef.type) {
      case 'json':
        newModel[columnName] = JSON.parse(values[columnName]);
        break;
      case 'boolean':
        newModel[columnName] = !!values[columnName];
        break;
      case 'number':
        newModel[columnName] = parseFloat(values[columnName]);
        break;
      case 'numberkey':
        newModel[columnName] = parseInt(values[columnName], 10);
        break;
      case 'string':
        newModel[columnName] = values[columnName].toString();
      default:
        newModel[columnName] = values[columnName];
    }
  }

  return newModel;
};

module.exports = Query;
