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
  this._escapedTable = utils.escapeTable(tableName);
  /** Waterline model - provides info on type */
  this._modelByColumnName = {};
  if (!!model) {
    for (let prop in model) {
      if (prop !== 'definition') {
        this._modelByColumnName[prop] = model[prop];
      } else {
        const definitions = this._modelByColumnName[prop] = {};
        const attrs = model[prop];

        for (let attrName in attrs) {
          const attrDef = attrs[attrName];
          definitions[attrDef.columnName] = attrDef;
        }
      }
    }
  }

  this._schema = _.clone(schema);

  return this;
};

/**
 * SELECT Statement
 */

Query.prototype.find = function(criteria = {}) {

  const selectKeys = [{table: this._escapedTable, key: 'rowid'}];
  if (criteria.select && criteria.select.length >>> 0 > 0) {
    for (let key of criteria.select) {
      selectKeys.push({ table: this._escapedTable, key });
    }
  } else {
    for (let columnName in this._schema) {
      selectKeys.push({ table: this._escapedTable, key: columnName });
    }
  }
  const selects =
    selectKeys
      .map(keyObj => `${keyObj.table}.${utils.escapeName(keyObj.key)}`)
      .join(', ');

  this._query = `SELECT ${selects} FROM ${this._escapedTable} `;

  this._build(criteria);

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * COUNT Statement
 * Waterline only supports counting based on criteria, so we only need
 * count(*) syntax
 */
Query.prototype.count = function(criteria = {}, alias = 'count_alias') {
  this._query = `SELECT COUNT(*) as ${alias} FROM ${this._escapedTable} `;
  this._build(criteria);

  return {
    query: this._query,
    values: this._values
  };
};

/**
 * SUM Statement
 * No Group By in Waterline api v1. Odd...
 */
Query.prototype.sum = function(criteria = {}, columnName, alias = 'sum_alias') {
  this._query = `SELECT TOTAL(${utils.escapeName(columnName)}) as ${alias} FROM ${this._escapedTable} `;
  this._build(criteria);

  return {
    query: this._query,
    values: this._values
  };
}

/**
 * AVG Statement
 * No Group By in Waterline api v1. Odd...
 */
Query.prototype.avg = function (criteria = {}, columnName, alias = 'avg_alias') {
  this._query = `SELECT AVG(${utils.escapeName(columnName)}) as ${alias} FROM ${this._escapedTable} `;
  this._build(criteria);

  return {
    query: this._query,
    values: this._values
  };
}

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
  if (criteria) {
    criteria = Object.assign({}, criteria);
    delete criteria.limit; //we don't want a limit in a delete query
    this._build(criteria);
  }

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

    criteriaTree.setParamIndex(this._paramCount);

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

  setParamIndex(index) {
    this._paramIndex = index;
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
    let isRoot = false;
    if (paramIndex > 0) {
      this._paramIndex = paramIndex;
    } else {
      isRoot = true;
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

      const whereClause = (isRoot ? 'WHERE ' : '')
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
          whereClause = `${columnName} NOT IN ${setString}`;
          break;
      }
    }

    return {whereClause, values, paramIndex: this._paramIndex};
  }
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
    const attrDef = this._modelByColumnName.definition[columnName];
    const value = values[columnName];

    if (value === null) {
      newModel[columnName] = null;
      continue;
    }

    switch(attrDef.type) {
      case 'json':
        let parsedVal;
        try {
          parsedVal = JSON.parse(value);
        } catch (err) {
          // edge case of just string
          if (!value.startsWith('{') && !value.startsWith('[')) {
            parsedVal = JSON.parse(`"${value}"`);
          } else {
            throw err;
          }
        }

        newModel[columnName] = parsedVal;
        break;
      case 'boolean':
        newModel[columnName] = !!value;
        break;
      case 'number':
        newModel[columnName] = parseFloat(value);
        break;
      case 'numberkey':
        newModel[columnName] = parseInt(value, 10);
        break;
      case 'string':
        newModel[columnName] = value.toString();
      default:
        newModel[columnName] = value;
    }
  }

  return newModel;
};

module.exports = Query;
