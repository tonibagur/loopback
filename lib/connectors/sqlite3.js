var SqlConnector = require('loopback-connector').SqlConnector,
    util  = require('util'),
    async = require('async'),
    debug = require('debug')('loopback:connector:sqlite');
    var loopback = require('loopback');
    var boot = require('loopback-boot');


/**
 *
 * Initialize the PostgreSQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header PostgreSQL.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  
  console.log('initialize');
  /*var dbSettings = dataSource.settings || {};
  dbSettings.host = dbSettings.host || dbSettings.hostname || 'localhost';
  dbSettings.user = dbSettings.user || dbSettings.username;
  dbSettings.debug = dbSettings.debug || debug.enabled;

  dataSource.connector = new PostgreSQL(postgresql, dbSettings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    dataSource.connecting = true;
    dataSource.connector.connect(callback);
  }*/

};

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(Sqlite3, SqlConnector);

module.exports = Sqlite3;
var db=null;
//var settings = require('datasources.json')

	function Sqlite3() {
		SqlConnector.call(this, 'sqlite3', {});
		//this.settings = settings.sqlite;
		//this.file_name = this.settings.file_name;
	};

	Sqlite3.prototype.connect = function (callback) {
		try {
      db = window.sqlitePlugin.openDatabase({name: "basedades.sqlite3", location: 1, androidDatabaseImplementation: 2,
      androidLockWorkaround: 1},callback);
 
    }
    catch(err) {
      
    }   
     
  };

	Sqlite3.prototype.disconnect = function(callback) {
		console.log('disconnecting');
		db.close(callback);
	};


	Sqlite3.prototype.ping = function(callback) {
		this.executeSql('SELECT 100 AS result', [], callback);
	};

	Sqlite3.prototype.executeSQL = function (sql, params, options, callback) {
    console.log(sql);
    db.transaction(function(tx) {
      //tx.executeSql('DROP TABLE IF EXISTS test_table');
      tx.executeSql(sql,params, function(tx, res) {
          var rows=res.rows;
          if(sql.startsWith('PRAGMA')){
             var new_rows = [];
             
             for(var i=0; i < rows.length; i++) {
                var temp = {};
                temp['column'] = rows.item(i).name;
                temp['type'] = rows.item(i).type;
                temp['nullable'] = (rows.item(i).notnull == 0) ? 'YES' : 'NO';
                new_rows.push(temp);
              }

              callback(false, new_rows);
          }
          else if(sql.startsWith('INSERT')){
            if(callback){ 
              callback(false,res);
            }
          }
          else if(callback){ 
            var result=[];
            for(var i=0; i < rows.length; i++)
              result.push(rows.item(i));
            callback(false,result);
          }
        });

    }, function(e) {
        console.log("ERROR: " + e.message);
    });
   
  };

  /**
   * Parse the result for SQL UPDATE/DELETE/INSERT for the number of rows
   * affected
   * @param {String} model Model name
   * @param {Object} info Status object
   * @returns {Number} Number of rows affected
   */
  Sqlite3.prototype.getCountForAffectedRows = function(model, info) {
    return info.rowsAffected;
  };

	/**
   * Perform autoupdate for the given models
   * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
   * @callback {Function} [callback] The callback function
   * @param {String|Error} err The error string or object
   */
  Sqlite3.prototype.autoupdate = function(models, cb) {
      var self = this;
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(this._models);
    async.each(models, function(model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function() {
          done(new Error('Model not found: ' + model));
        });
      }
      getTableStatus.call(self, model, function(err, fields) {
        if (!err && fields.length) {
          self.alterTable(model, fields, done);
        } else {
          self.createTable(model, done);
        }
      });
    }, cb);
  };

  /*!
 * Alter the table for the given model
 * @param {String} model The model name
 * @param {Object[]} actualFields Actual columns in the table
 * @param {Function} [cb] The callback function
 */
Sqlite3.prototype.alterTable = function (model, actualFields, cb) {
  var self = this;
  var pendingChanges = getAddModifyColumns.call(self, model, actualFields);
  if (pendingChanges.length > 0) {
    applySqlChanges.call(self, model, pendingChanges, function (err, results) {
      var dropColumns = getDropColumns.call(self, model, actualFields);
      if (dropColumns.length > 0) {
        applySqlChanges.call(self, model, dropColumns, cb);
      } else {
        cb && cb(err, results);
      }
    });
  } else {
    var dropColumns = getDropColumns.call(self, model, actualFields);
    if (dropColumns.length > 0) {
      applySqlChanges.call(self, model, dropColumns, cb);
    } else {
      cb && process.nextTick(cb.bind(null, null, []));
    }
  }
};

function getAddModifyColumns(model, actualFields) {
  var sql = [];
  var self = this;
  sql = sql.concat(getColumnsToAdd.call(self, model, actualFields));
  var drops = getPropertiesToModify.call(self, model, actualFields);
  if (drops.length > 0) {
    if (sql.length > 0) {
      sql = sql.concat(', ');
    }
    sql = sql.concat(drops);
  }
  // sql = sql.concat(getColumnsToDrop.call(self, model, actualFields));
  return sql;
}

function getDropColumns(model, actualFields) {
  var sql = [];
  var self = this;
  // sql = sql.concat(getColumnsToDrop.call(self, model, actualFields));
  return sql;
}

function getColumnsToAdd(model, actualFields) {
  var self = this;
  var m = self._models[model];
  var propNames = Object.keys(m.properties);
  var sql = [];
  propNames.forEach(function (propName) {
    if (self.id(model, propName)) return;
    var found = searchForPropertyInActual.call(self, model, self.column(model, propName), actualFields);

    if (!found && propertyHasNotBeenDeleted.call(self, model, propName)) {
      sql.push('ADD COLUMN ' + addPropertyToActual.call(self, model, propName));
    }
  });

  return sql;
}
function addPropertyToActual(model, propName) {
  var self = this;
  var sqlCommand = self.columnEscaped(model, propName)
    + ' ' + self.columnDataType(model, propName) + (propertyCanBeNull.call(self, model, propName) ? "" : " NOT NULL");
  return sqlCommand;
}


function searchForPropertyInActual(model, propName, actualFields) {
  var self = this;
  var found = false;
  actualFields.forEach(function (f) {
    if (f.column === self.column(model, propName)) {
      found = f;
      return;
    }
  });
  return found;
}
function propertyHasNotBeenDeleted(model, propName) {
  return !!this._models[model].properties[propName];
}
function getPropertiesToModify(model, actualFields) {
  var self = this;
  var sql = [];
  var m = self._models[model];
  var propNames = Object.keys(m.properties);
  var found;
  propNames.forEach(function (propName) {
    if (self.id(model, propName)) {
      return;
    }
    found = searchForPropertyInActual.call(self, model, propName, actualFields);
    if (found && propertyHasNotBeenDeleted.call(self, model, propName)) {
      if (datatypeChanged(propName, found)) {
        sql.push('ALTER COLUMN ' + modifyDatatypeInActual.call(self, model, propName));
      }
      if (nullabilityChanged(propName, found)) {
        sql.push('ALTER COLUMN' + modifyNullabilityInActual.call(self, model, propName));
      }
    }
  });

  if (sql.length > 0) {
    sql = [sql.join(', ')];
  }

  return sql;

  function datatypeChanged(propName, oldSettings) {
    var newSettings = m.properties[propName];
    if (!newSettings) {
      return false;
    }
    return oldSettings.type.toUpperCase() !== self.columnDataType(model, propName);
  }

  function isNullable(p) {
    return !(p.required ||
      p.id ||
      p.allowNull === false ||
      p.null === false ||
      p.nullable === false);
  }

  function nullabilityChanged(propName, oldSettings) {
    var newSettings = m.properties[propName];
    if (!newSettings) {
      return false;
    }
    var changed = false;
    if (oldSettings.nullable === 'YES' && !isNullable(newSettings)) {
      changed = true;
    }
    if (oldSettings.nullable === 'NO' && isNullable(newSettings)) {
      changed = true;
    }
    return changed;
  }
}

function modifyDatatypeInActual(model, propName) {
  var self = this;
  var sqlCommand = self.columnEscaped(model, propName) + ' TYPE ' +
    self.columnDataType(model, propName);
  return sqlCommand;
}

function modifyNullabilityInActual(model, propName) {
  var self = this;
  var sqlCommand = self.columnEscaped(model, propName) + ' ';
  if (propertyCanBeNull.call(self, model, propName)) {
    sqlCommand = sqlCommand + "DROP ";
  } else {
    sqlCommand = sqlCommand + "SET ";
  }
  sqlCommand = sqlCommand + "NOT NULL";
  return sqlCommand;
}

function getColumnsToDrop(model, actualFields) {
  var self = this;
  var sql = [];
  actualFields.forEach(function (actualField) {
    if (self.idColumn(model) === actualField.column) {
      return;
    }
    if (actualFieldNotPresentInModel(actualField, model)) {
      sql.push('DROP COLUMN ' + self.escapeName(actualField.column));
    }
  });
  if (sql.length > 0) {
    sql = [sql.join(', ')];
  }
  return sql;

  function actualFieldNotPresentInModel(actualField, model) {
    return !(self.propertyName(model, actualField.column));
  }
}

function applySqlChanges(model, pendingChanges, cb) {
  var self = this;
  if (pendingChanges.length) {
    var thisQuery = 'ALTER TABLE ' + self.tableEscaped(model);
    var ranOnce = false;
    pendingChanges.forEach(function (change) {
      if (ranOnce) {
        thisQuery = thisQuery + '; ';
      }
      thisQuery = thisQuery + ' ' + change;
      ranOnce = true;
    });
    // thisQuery = thisQuery + ';';
    self.query(thisQuery, cb);
  }
}

Sqlite3.prototype.toColumnValue = function(prop, val) {
  return val;
};

Sqlite3.prototype.getPlaceholderForValue = function(key) {
  return '$' + key;
};

Sqlite3.prototype._buildLimit = function(model, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  return 'LIMIT ' + (offset ? (offset + ',' + limit) : limit);
};

Sqlite3.prototype.applyPagination =
  function(model, stmt, filter) {
    /*jshint unused:false */
    var limitClause = this._buildLimit(model, filter.limit,
      filter.offset || filter.skip);
    return stmt.merge(limitClause);
  };

  Sqlite3.prototype.escapeName = function (name) {
    if (!name) {
      return name;
    }
    return '"' + name.replace(/\./g, '"."') + '"';
  };


	/*Sqlite3.prototype.autoupdate = function(model, callback) {
    var db = window.sqlitePlugin.openDatabase({name: "basedades.sqlite3", location: 1, androidDatabaseImplementation: 2,
     androidLockWorkaround: 1});

    db.transaction(function(tx) {
      //tx.executeSql('DROP TABLE IF EXISTS test_table');
      tx.executeSql('CREATE TABLE IF NOT EXISTS test_table (id integer primary key, data text, data_num integer)');

      tx.executeSql("INSERT INTO test_table (data, data_num) VALUES (?,?)", ["test", 100], function(tx, res) {
        console.log("insertId: " + res.insertId + " -- probably 1");
        console.log("rowsAffected: " + res.rowsAffected + " -- should be 1");

        tx.executeSql("select * from test_table;", [], function(tx, res) {
          console.log('select *');
          for(var i=0;i<res.rows.length;i++){
              console.log(res.rows.item(i));
          }
        });

      }, function(e) {
        console.log("ERROR: " + e.message);
      });
    });
	};*/

function mapSQLiteDatatypes(typeName) {
  return typeName;
};

  /*!
 * Discover the properties from a table
 * @param {String} model The model name
 * @param {Function} cb The callback function
 */
function getTableStatus(model, cb) {
  function decoratedCallback(err, data) {
    if (err) {
      console.error(err);
    }
    if (!err) {
      data.forEach(function (field) {
        field.type = mapSQLiteDatatypes(field.type);
      });
    }
    cb(err, data);
  }
  var sql = null;
  sql = 'PRAGMA table_info(' + this.table(model) +')';
  var params = [];
  params.status = true;
  this.executeSQL(sql, params,false, decoratedCallback);
}

/*!
 * Build a list of columns for the given model
 * @param {String} model The model name
 * @returns {String}
 */
 Sqlite3.prototype.buildColumnDefinitions =  function (model) {
  var self = this;
  var sql = [];
  var pks = this.idNames(model).map(function (i) {
    return self.columnEscaped(model, i);
  });
  Object.keys(this._models[model].properties).forEach(function (prop) {
    var colName = self.columnEscaped(model, prop);
    sql.push(colName + ' ' + self.buildColumnDefinition(model, prop));
  });
  if (pks.length > 0) {
    sql.push('PRIMARY KEY(' + pks.join(',') + ')');
  }
  return sql.join(',\n  ');
};

/*!
 * Build settings for the model property
 * @param {String} model The model name
 * @param {String} propName The property name
 * @returns {*|string}
 */
Sqlite3.prototype.buildColumnDefinition = function (model, propName) {
  var self = this;
  if (this.id(model, propName) && this._models[model].properties[propName].generated) {
    return 'INTEGER';
  }
  var result = self.columnDataType(model, propName);
  if (!propertyCanBeNull.call(self, model, propName)) result = result + ' NOT NULL';

  result += self.columnDbDefault(model, propName);
  return result;
};

/*!
 * Get the database-default value for column from given model property
 *
 * @param {String} model The model name
 * @param {String} property The property name
 * @returns {String} The column default value
 */
Sqlite3.prototype.columnDbDefault = function(model, property) {
  var columnMetadata = this.columnMetadata(model, property);
  var colDefault = columnMetadata && columnMetadata.dbDefault;

  return colDefault ? (' DEFAULT ' + columnMetadata.dbDefault): '';
};


/*!
 * Find the column type for a given model property
 *
 * @param {String} model The model name
 * @param {String} property The property name
 * @returns {String} The column type
 */
Sqlite3.prototype.columnDataType = function (model, property) {
  var columnMetadata = this.columnMetadata(model, property);
  var colType = columnMetadata && columnMetadata.dataType;
  if (colType) {
    colType = colType.toUpperCase();
  }
  var prop = this._models[model].properties[property];
  if (!prop) {
    return null;
  }
  var colLength = columnMetadata && columnMetadata.dataLength || prop.length;
  if (colType) {
    return colType + (colLength ? '(' + colLength + ')' : '');
  }

  switch (prop.type.name) {
    default:
    case 'String':
    case 'JSON':
      return 'TEXT';
    case 'Text':
      return 'TEXT';
    case 'Number':
      return 'INTEGER';
    case 'Date':
      return 'DATETIME';
    case 'Timestamp':
      return 'DATETIME';
    case 'GeoPoint':
    case 'Point':
      return 'POINT';
    case 'Boolean':
      return 'BOOLEAN';
  }
};

  Sqlite3.prototype.getInsertedId = function(model, info) {
    return info.id;
  };

  Sqlite3.prototype.fromColumnValue = function(propertyDef, value) {
    return value;
  };

function propertyCanBeNull(model, propName) {
  var p = this._models[model].properties[propName];
  if (p.required || p.id) {
    return false;
  }
  return !(p.allowNull === false ||
    p['null'] === false || p.nullable === false);
}

function generateQueryParams(data, props) {
  var queryParams = [];

  function pushToQueryParams(key) {
    queryParams.push(data[key] !== undefined ? data[key] : null);
  }

  props.nonIdsInData.forEach(pushToQueryParams);
  props.idsInData.forEach(pushToQueryParams);

  return queryParams;
}

if (!String.prototype.startsWith) {
  String.prototype.startsWith = function(searchString, position) {
    position = position || 0;
    return this.lastIndexOf(searchString, position) === position;
  };
}
