/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * - Neither Hypertable, Inc. nor the names of its contributors may be used to
 *   endorse or promote products derived from this software without specific
 *   prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
'use strict';
var assert = require('assert');
var async = require('async');
var util = require('util');
var hypertable = require('hypertable');
var Timestamp = require('hypertable/timestamp');

/**
 * This callback type is called `requestCallback` and is displayed as a global
 * symbol.
 * @callback requestCallback
 * @param {Object} exception Exception object
 * @param {string} responseMessage Response message
 */

/**
 * Hypertable client
 * @type {Object}
 */
var client = new hypertable.ThriftClient("localhost", 15867);

var testBasic = function(callback) {
  console.log('[basic]');
  var ns;
  async.series([
    function createTestNamespace (callback) {
      client.namespace_exists('test', function (error, exists) {
          if (!exists)
            client.namespace_create('test', callback);
          else
            callback(error);
        });
    },
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function dropFruitsTable (callback) {
      var ifExists = true;
      client.table_drop(ns, 'Fruits', ifExists, callback);
    },
    function createFruitsTable (callback) {
      var columnFamilies = {};
      columnFamilies.genus = new hypertable.ColumnFamilySpec({name: 'genus'});
      columnFamilies.description = new hypertable.ColumnFamilySpec({name: 'description'});
      columnFamilies.tag = new hypertable.ColumnFamilySpec({name: 'tag'});
      var schema = new hypertable.Schema({column_families: columnFamilies});
      client.table_create(ns, 'Fruits', schema, callback);
    },
    function createTestSubNamespace (callback) {
      client.namespace_create('/test/sub', callback);
    },
    function displayTestListing (callback) {
      client.namespace_get_listing(ns, function (error, listing) {
          for (var j = 0; j < listing.length; j++) {
            if (listing[j].is_namespace)
              console.log(util.format('%s\t(dir)', listing[j].name));
            else
              console.log(util.format('%s', listing[j].name));
          }
          callback(error);
        });
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testBasic');
  });
};


var testConvenience = function(callback) {

  async.series([
    function setCellsExample (callback) {
      console.log('[set_cells]');
      var ns;
      async.series([
        function openTestNamespace (callback) {
          client.namespace_open('test', function (error, result) {
              if (!error)
                ns = result;
              callback(error);
            });
        },
        function setCells (callback) {
          var key, cell;
          var cells = [];

          key = new hypertable.Key({row: 'apple', column_family: 'genus'});
          cell = new hypertable.Cell({key: key, value: 'Malus'});
          cells.push(cell);

          key = new hypertable.Key({row: 'apple', column_family: 'description'});
          cell = new hypertable.Cell({key: key,
                                      value: 'The apple is the pomaceous fruit of the apple tree.'});
          cells.push(cell);

          key = new hypertable.Key({row: 'apple', column_family: 'tag', column_qualifier: 'crunchy'});
          cell = new hypertable.Cell({key: key});
          cells.push(cell);
    
          client.set_cells(ns, 'Fruits', cells,
                           function (error) { callback(error); });
        },
        function closeTestNamespace (callback) {
          client.namespace_close(ns, callback);
        }
      ],
      function(error) {
        callback(error);
      });
    },
    function getCellsExample (callback) {
      console.log('[get_cells]');
      var ns;
      async.series([
        function openTestNamespace (callback) {
          client.namespace_open('test', function (error, result) {
              if (!error)
                ns = result;
              callback(error);
            });
        },
        function getCells (callback) {
          client.get_cells(ns, 'Fruits', new ScanSpec({columns: ['description']}),
                           function (error, cells) {
              if (!error) {
                for (var j = 0; j < cells.length; j++)
                  console.log(cells[j].toString());
              }
              callback(error);
            });
        },
        function closeTestNamespace (callback) {
          client.namespace_close(ns, callback);
        }
      ],
      function(error) {
        callback(error);
      });
    },
    function setCellsAsArraysExample (callback) {
      console.log('[set_cells_as_arrays]');
      var ns;
      async.series([
        function openTestNamespace (callback) {
          client.namespace_open('test', function (error, result) {
              if (!error)
                ns = result;
              callback(error);
            });
        },
        function setCellsAsArrays (callback) {
          var cell_as_array;
          var cells_as_arrays = [];

          cell_as_array = ['orange', 'genus', '', 'Citrus'];
          cells_as_arrays.push(cell_as_array);

          cell_as_array = ['orange', 'description', '',
                           'The orange (specifically, the sweet orange) is ' +
                           'the fruit of the citrus species Citrus x ' +
                           'sinensis in the family Rutaceae.'];
          cells_as_arrays.push(cell_as_array);

          cell_as_array = ['orange', 'tag', 'juicy', ''];
          cells_as_arrays.push(cell_as_array);
    
          client.set_cells_as_arrays(ns, 'Fruits', cells_as_arrays, callback);
        },
        function closeTestNamespace (callback) {
          client.namespace_close(ns, callback);
        }
      ],
      function(error) {
        callback(error);
      });
    },
    function getCellsAsArraysExample (callback) {
      console.log('[get_cells_as_arrays]');
      var ns;
      async.series([
        function openTestNamespace (callback) {
          client.namespace_open('test', function (error, result) {
              if (!error)
                ns = result;
              callback(error);
            });
        },
        function getCellsAsArrays (callback) {
          client.get_cells_as_arrays(ns, 'Fruits', new ScanSpec({columns: ['description']}),
                           function (error, cells_as_arrays) {
              if (!error) {
                for (var j = 0; j < cells_as_arrays.length; j++)
                  console.log(cells_as_arrays[j].slice(0 ,4));
              }
              callback(error);
            });
        },
        function closeTestNamespace (callback) {
          client.namespace_close(ns, callback);
        }
      ],
      function(error) {
        callback(error);
      });
    },
    function setCellsSerializedExample (callback) {
      console.log('[set_cells_serialized]');
      var ns;
      async.series([
        function openTestNamespace (callback) {
          client.namespace_open('test', function (error, result) {
              if (!error)
                ns = result;
              callback(error);
            });
        },
        function setCellsSerialized (callback) {

          var writer = new hypertable.SerializedCellsWriter(1024);

          writer.add('canteloupe', 'genus', null, null, 'Cucumis');

          writer.add('canteloupe', 'description', null, null,
                     'Canteloupe refers to a variety of Cucumis melo, a ' +
                     'species in the family Cucurbitaceae.');

          writer.add('canteloupe', 'tag', 'juicy');
          
          client.set_cells_serialized(ns, 'Fruits', writer.getBuffer(), callback);

        },
        function closeTestNamespace (callback) {
          client.namespace_close(ns, callback);
        }
      ],
      function(error) {
        callback(error);
      });
    },
    function getCellsSerializedExample (callback) {
      console.log('[get_cells_serialized]');
      var ns;
      async.series([
        function openTestNamespace (callback) {
          client.namespace_open('test', function (error, result) {
              if (!error)
                ns = result;
              callback(error);
            });
        },
        function getCellsSerialized (callback) {
          client.get_cells_serialized(ns, 'Fruits',
                                      new ScanSpec({columns: ['description']}),
                                      function (error, result) {
                                        if (!error) {
                                          var reader = new hypertable.SerializedCellsReader(result);
                                          while (reader.next())
                                            console.log(reader.getCell().toString());
                                        }
                                        callback(error);
                                      });
        },
        function closeTestNamespace (callback) {
          client.namespace_close(ns, callback);
        }
      ],
      function(error) {
        callback(error);
      });
    }
  ],
  function(error) {
    callback(error, 'testConvenience');
  });
};

var testCreateTable = function(callback) {
  console.log('[create table]');
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function createTable (callback) {

      var defaultAgOptions = new hypertable.AccessGroupOptions({blocksize: 65536});
      var defaultCfOptions = new hypertable.ColumnFamilyOptions({max_versions: 1});
  
      var cfOptions = new hypertable.ColumnFamilyOptions({max_versions: 2});
      var agSpec = new hypertable.AccessGroupSpec({name: 'ag_normal', defaults: cfOptions});

      var agSpecs = {};
      agSpecs['ag_normal'] = agSpec;
  
      var cfSpec = new hypertable.ColumnFamilySpec({name: 'a', access_group: 'ag_normal',
                                                    value_index: true, qualifier_index: true});
      var cfSpecs = {};
      cfSpecs['a'] = cfSpec;

      cfOptions = new hypertable.ColumnFamilyOptions({max_versions: 3});
      cfSpec = new hypertable.ColumnFamilySpec({name: 'b', access_group: 'ag_normal',
                                                options: cfOptions});
      cfSpecs['b'] = cfSpec;

      var agOptions = new hypertable.AccessGroupOptions({in_memory: true, blocksize: 131072});
      agSpec = new hypertable.AccessGroupSpec({name: 'ag_fast', options: agOptions});
      agSpecs['ag_fast'] = agSpec;
  
      cfSpec = new hypertable.ColumnFamilySpec({name: 'c', access_group: 'ag_fast'});
      cfSpecs['c'] = cfSpec;

      agOptions = new hypertable.AccessGroupOptions({replication: 5});
      agSpec = new hypertable.AccessGroupSpec({name: 'ag_secure', options: agOptions});
      agSpecs['ag_secure'] = agSpec;

      cfSpec = new hypertable.ColumnFamilySpec({name: 'd', access_group: 'ag_secure'});
      cfSpecs['d'] = cfSpec;

      cfOptions = new hypertable.ColumnFamilyOptions({counter: true, max_versions: 0});
      agSpec = new hypertable.AccessGroupSpec({name: 'ag_counter', defaults: cfOptions});
      agSpecs['ag_counter'] = agSpec;

      cfSpec = new hypertable.ColumnFamilySpec({name: 'e', access_group: 'ag_counter'});
      cfSpecs['e'] = cfSpec;

      cfOptions = new hypertable.ColumnFamilyOptions({counter: false});
      cfSpec = new hypertable.ColumnFamilySpec({name: 'f', access_group: 'ag_counter',
                                                options: cfOptions});
      cfSpecs['f'] = cfSpec;
  
      var schema = new hypertable.Schema({access_groups: agSpecs, column_families: cfSpecs,
                                          access_group_defaults: defaultAgOptions,
                                          column_family_defaults: defaultCfOptions});
      
      client.table_create(ns, 'TestTable', schema, callback);
    },
    function showCreateTable (callback) {
      client.hql_query(ns, "SHOW CREATE TABLE TestTable", function (error, result) {
          if (result)
            console.log(util.format('%s', result.results[0]));
          callback(error);
        });
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testCreateTable');
  });
};


var testAlterTable = function(callback) {
  console.log('[alter table]');
  var ns;
  var schema;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function getSchema (callback) {
      client.get_schema(ns, 'TestTable', function (error, result) {
          if (!error)
            schema = result;
          callback(error);
        });
    },
    function alterTable (callback) {

      // Rename column 'b' to 'z'
      var cfSpec = schema.column_families['b'];
      delete schema.column_families['b'];
      cfSpec.name = 'z';
      schema.column_families['z'] = cfSpec;

      // Add column "g"
      cfSpec = new hypertable.ColumnFamilySpec({name: 'g', access_group: 'ag_counter'});
      schema.column_families['g'] = cfSpec;

      client.table_alter(ns, 'TestTable', schema, callback);
    },
    function showCreateTable (callback) {
      client.hql_query(ns, "SHOW CREATE TABLE TestTable", function (error, result) {
          if (result)
            console.log(util.format('%s', result.results[0]));
          callback(error);
        });
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAlterTable');
  });
};


var testMutator = function(callback) {
  console.log('[mutator]');
  var mutator;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openMutator (callback) {
      client.mutator_open(ns, "Fruits", 0, 0, function (error, result) {
          if (!error)
            mutator = result;
          callback(error);
        });
    },
    function setCellsAutoTimestamp (callback) {
      var cells = [];
      var cell;
      var key;

      key = new hypertable.Key({row: 'lemon', column_family: 'genus'});
      cell = new hypertable.Cell({key: key, value: 'Citrus'});
      cells.push(cell);

      key = new hypertable.Key({row: 'lemon', column_family: 'description'});
      cell = new hypertable.Cell({key: key, value: 'The lemon (Citrus x limon)' +
                                  'is a small evergreen tree native to Asia.'});
      cells.push(cell);

      key = new hypertable.Key({row: 'lemon', column_family: 'tag',
                                column_qualifier: 'bitter'});
      cell = new hypertable.Cell({key: key});
      cells.push(cell);

      client.mutator_set_cells(mutator, cells, callback);

    },
    function flushMutator (callback) {
      client.mutator_flush(mutator, callback);
    },
    function setCellsWithTimestamp (callback) {
      var cells = [];
      var cell;
      var key;
      var ts;

      // 2014-06-06 16:27:15
      ts = new Timestamp( new Date(2014, 6, 6, 16, 27, 15) );

      key = new hypertable.Key({row: 'mango', column_family: 'genus',
                                timestamp: ts});
      cell = new hypertable.Cell({key: key, value: 'Mangifera'});
      cells.push(cell);

      key = new hypertable.Key({row: 'mango', column_family: 'tag',
                                column_qualifier: 'sweet',
                                timestamp: ts});
      cell = new hypertable.Cell({key: key});
      cells.push(cell);

      key = new hypertable.Key({row: 'mango', column_family: 'description',
                                timestamp: ts});
      cell = new hypertable.Cell({key: key, value: 'Mango is one of the ' +
                                  'delicious seasonal fruits grown in the ' +
                                  'tropics.'});
      cells.push(cell);

      // 2014-06-06 16:27:16
      ts = new Timestamp( new Date(2014, 6, 6, 16, 27, 16) );

      key = new hypertable.Key({row: 'mango', column_family: 'description',
                                timestamp: ts});
      cell = new hypertable.Cell({key: key, value: 'The mango is a juicy stone' +
                                  ' fruit belonging to the genus Mangifera, ' +
                                  'consisting of numerous tropical fruiting ' +
                                  'trees, that are cultivated mostly for ' +
                                  'edible fruits.'});
      cells.push(cell);

      client.mutator_set_cells(mutator, cells, callback);
    },
    function flushMutator (callback) {
      client.mutator_flush(mutator, callback);
    },
    function setCellsDelete (callback) {
      var cells = [];
      var cell;
      var key;
      var ts;

      // 2014-06-06 16:27:15
      ts = new Timestamp( new Date(2014, 6, 6, 16, 27, 15) );

      key = new hypertable.Key({row: 'apple', flag: hypertable.KeyFlag.DELETE_ROW});
      cell = new hypertable.Cell({key: key});
      cells.push(cell);

      key = new hypertable.Key({row: 'mango', column_family: 'description',
                                flag: hypertable.KeyFlag.DELETE_CELL});
      cell = new hypertable.Cell({key: key});
      cells.push(cell);

      client.mutator_set_cells(mutator, cells, callback);

    },
    function flushMutator (callback) {
      client.mutator_flush(mutator, callback);
    },
    function closeMutator (callback) {
      client.mutator_close(mutator, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testMutator');
  });
};

var testScannerFullTable = function(callback) {
  console.log('[scanner full table scan]');
  var ns;
  var scanSpec;
  var scanner;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {
      scanSpec = new hypertable.ScanSpec();
      client.scanner_open(ns, "Fruits", scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function doScan (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testScannerFullTable');
  });
};

var testScannerRestricted = function(callback) {
  console.log('[scanner restricted scan]');
  var ns;
  var scanSpec;
  var scanner;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {
      var rowInterval = new hypertable.RowInterval({start_row: 'cherry',
                                                    start_inclusive: true,
                                                    end_row: 'orange',
                                                    end_inclusive: false});
      var rowIntervals = [ rowInterval ];
      var columns = ['genus', 'tag:fleshy', 'tag:bitter', 'tag:sweet'];
      scanSpec = new hypertable.ScanSpec({row_intervals: rowIntervals,
                                          versions: 1, columns: columns});
      client.scanner_open(ns, "Fruits", scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function doScan (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testScannerRestricted');
  });
};

var testHql = function(callback) {
  console.log('[HQL query]');
  var ns = 0;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function getListing (callback) {
      client.hql_query(ns, 'GET LISTING', function (error, response) {
          if (!error) {
            for (var j = 0; j < response.results.length; j++)
              console.log(util.format('%s', response.results[j]));
          }
          callback(error);
        });
    },
    function select (callback) {
      client.hql_query(ns, "SELECT * from Fruits WHERE ROW = 'mango'", function (error, response) {
          if (!error) {
            for (var j = 0; j < response.cells.length; j++)
              console.log(response.cells[j].toString());
          }
          callback(error);
        });
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testHql');
  });
};


var testHqlAsArrays = function(callback) {
  console.log('[HQL query as arrays]');
  var ns = 0;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlQueryAsArrays (callback) {
      client.hql_query_as_arrays(ns, "SELECT * from Fruits WHERE ROW = 'lemon'", function (error, result) {
          if (!error) {
            for (var j = 0; j < result.cells.length; j++)
              console.log(result.cells[j].slice(0 ,4));
          }
          callback(error);
        });
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testHqlAsArrays');
  });
};


var testHqlExecMutator = function(callback) {
  console.log('[HQL exec mutator]');
  var mutator;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlExecMutator (callback) {
      client.hql_exec(ns, "INSERT INTO Fruits VALUES ('strawberry', 'genus'," +
                      "'Fragaria'), ('strawberry', 'tag:fibrous', ''), " +
                      "('strawberry', 'description', 'The garden strawberry " +
                      "is a widely grown hybrid species of the genus " +
                      "Fragaria')", true, false, function (error, result) {
          if (!error)
            mutator = result.mutator;
          callback(error);
        });
    },
    function mutatorSetCells (callback) {
      var cells = [];
      var cell;
      var key;

      key = new hypertable.Key({row: 'pineapple', column_family: 'genus'});
      cell = new hypertable.Cell({key: key, value: 'Anans'});
      cells.push(cell);

      key = new hypertable.Key({row: 'pineapple', column_family: 'tag',
                                column_qualifier: 'acidic'});
      cell = new hypertable.Cell({key: key});
      cells.push(cell);

      key = new hypertable.Key({row: 'pineapple', column_family: 'description'});
      cell = new hypertable.Cell({key: key, value:
                                  'The pineapple (Ananas comosus) is a ' +
                                  'tropical plant with edible multiple fruit ' +
                                  'consisting of coalesced berries.'});
      cells.push(cell);

      client.mutator_set_cells(mutator, cells, callback);
    },
    function flushMutator (callback) {
      client.mutator_flush(mutator, callback);
    },
    function closeMutator (callback) {
      client.mutator_close(mutator, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testHqlExecMutator');
  });
};


var testHqlExecScanner = function(callback) {
  console.log('[HQL exec scanner]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlExecScanner (callback) {
      client.hql_exec(ns, 'SELECT * from Fruits', false, true, function (error, result) {
          if (!error)
            scanner = result.scanner;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testHqlExecScanner');
  });
};


var testSecondaryIndexSetup = function(callback) {
  console.log('[secondary indices]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlCreateTable (callback) {
      client.hql_query(ns, 'CREATE TABLE products (title, section, info, ' +
                       'category, INDEX section, INDEX info, ' +
                       'QUALIFIER INDEX info, QUALIFIER INDEX category)',
                       callback);
    },
    function hqlLoadData (callback) {
      client.hql_query(ns, 'LOAD DATA INFILE "indices_test_products.tsv" INTO TABLE products', callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryIndexSetup');
  });
};


var testSecondaryValueIndexExactMatch = function(callback) {
  console.log('[secondary index - SELECT title FROM products WHERE section = "books"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var args = {column_family: 'section',
                  operation: hypertable.ColumnPredicateOperation.EXACT_MATCH,
                  value: 'books'};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      var columns = [ 'title' ];
      var scanSpec = new hypertable.ScanSpec({column_predicates: [columnPredicate], columns: columns});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexExactMatch');
  });
};


var testSecondaryValueIndexExactMatchWithQualifier = function(callback) {
  console.log('[secondary index - SELECT title FROM products WHERE info:actor = "Jack Nicholson"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var operation = hypertable.ColumnPredicateOperation.EXACT_MATCH |
                      hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'actor',
                  operation: operation, value: 'Jack Nicholson'};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      var columns = [ 'title' ];
      var scanSpec = new hypertable.ScanSpec({column_predicates: [columnPredicate], columns: columns});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexExactMatchWithQualifier');
  });
};


var testSecondaryValueIndexPrefixMatch = function(callback) {
  console.log('[secondary index - SELECT title,info:publisher FROM products ' +
              'WHERE info:publisher =^ "Addison-Wesley"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var operation = hypertable.ColumnPredicateOperation.PREFIX_MATCH |
                      hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'publisher',
                  operation: operation, value: 'Addison-Wesley'};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      var columns = [ 'title', 'info:publisher' ];
      var scanSpec = new hypertable.ScanSpec({column_predicates: [columnPredicate], columns: columns});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexPrefixMatch');
  });
};


var testSecondaryValueIndexRegexMatch = function(callback) {
  console.log('[secondary index - SELECT title,info:publisher FROM products ' +
              'WHERE info:publisher =~ "^Addison-Wesley"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var operation = hypertable.ColumnPredicateOperation.REGEX_MATCH |
                      hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'publisher',
                  operation: operation, value: '^Addison-Wesley'};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      var columns = [ 'title', 'info:publisher' ];
      var scanSpec = new hypertable.ScanSpec({column_predicates: [columnPredicate], columns: columns});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexRegexMatch');
  });
};

var testSecondaryQualifierIndexExists = function(callback) {
  console.log('[secondary index - SELECT title FROM products ' +
              'WHERE Exists(info:studio)]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var operation = hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'studio',
                  operation: operation};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      var columns = [ 'title' ];
      var scanSpec = new hypertable.ScanSpec({column_predicates: [columnPredicate], columns: columns});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryQualifierIndexExists');
  });
};


var testSecondaryQualifierIndexExistsRegexPrefixMatch = function(callback) {
  console.log('[secondary index - SELECT title FROM products ' +
              'WHERE Exists(category:/^\\/Movies/)]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var operation = hypertable.ColumnPredicateOperation.QUALIFIER_REGEX_MATCH;
      var args = {column_family: 'category', column_qualifier: '^/Movies',
                  operation: operation};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      var columns = [ 'title' ];
      var scanSpec = new hypertable.ScanSpec({column_predicates: [columnPredicate], columns: columns});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryQualifierIndexExistsRegexPrefixMatch');
  });
};


var testSecondaryValueIndexOrQuery = function(callback) {
  console.log('[secondary index - SELECT title FROM products ' +
              'WHERE info:author =~ /^Stephen P/ OR ' +
              'info:publisher =^ "Anchor"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var predicates = [];
      var operation = hypertable.ColumnPredicateOperation.REGEX_MATCH |
        hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'author',
                  operation: operation, value: '^Stephen P'};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      predicates.push(columnPredicate);

      operation = hypertable.ColumnPredicateOperation.PREFIX_MATCH |
        hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      args = {column_family: 'info', column_qualifier: 'publisher',
                  operation: operation, value: 'Anchor'};
      columnPredicate = new hypertable.ColumnPredicate(args);
      predicates.push(columnPredicate);

      var scanSpec = new hypertable.ScanSpec({column_predicates: predicates,
                                              columns: [ 'title' ]});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexOrQuery');
  });
};

var testSecondaryValueIndexAndQuery = function(callback) {
  console.log('[secondary index - SELECT title FROM products ' +
              'WHERE info:author =~ /^Stephen [PK]/ AND ' +
              'info:publisher =^ "Anchor"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var predicates = [];
      var operation = hypertable.ColumnPredicateOperation.REGEX_MATCH |
        hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'author',
                  operation: operation, value: '^Stephen [PK]'};
      var columnPredicate = new hypertable.ColumnPredicate(args);
      predicates.push(columnPredicate);

      operation = hypertable.ColumnPredicateOperation.PREFIX_MATCH |
        hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      args = {column_family: 'info', column_qualifier: 'publisher',
                  operation: operation, value: 'Anchor'};
      columnPredicate = new hypertable.ColumnPredicate(args);
      predicates.push(columnPredicate);

      var scanSpec = new hypertable.ScanSpec({column_predicates: predicates,
                                              columns: [ 'title' ],
                                              and_column_predicates: true});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexAndQuery');
  });
};


var testSecondaryValueIndexAndRowInterval = function(callback) {
  console.log('[secondary index - SELECT title FROM products ' +
              'WHERE ROW > "B00002VWE0" AND info:actor = "Jack Nicholson"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var rowInterval = new hypertable.RowInterval({start_row: 'B00002VWE0',
                                                    start_inclusive: false,
                                                    end_inclusive: true});
      var operation = hypertable.ColumnPredicateOperation.EXACT_MATCH |
        hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'actor',
                  operation: operation, value: 'Jack Nicholson'};
      var columnPredicate = new hypertable.ColumnPredicate(args);

      var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval],
                                              column_predicates: [columnPredicate],
                                              columns: [ 'title' ],
                                              and_column_predicates: true});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexAndRowInterval');
  });
};


var testSecondaryValueIndexAndRowPrefix = function(callback) {
  console.log('[secondary index - SELECT title FROM products ' +
              'WHERE ROW =^ "B" AND info:actor = "Jack Nicholson"]');
  var scanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openScanner (callback) {

      var rowInterval = new hypertable.RowInterval({start_row: 'B',
                                                    start_inclusive: true,
                                                    end_row: 'C',
                                                    end_inclusive: false});
      var operation = hypertable.ColumnPredicateOperation.EXACT_MATCH |
        hypertable.ColumnPredicateOperation.QUALIFIER_EXACT_MATCH;
      var args = {column_family: 'info', column_qualifier: 'actor',
                  operation: operation, value: 'Jack Nicholson'};
      var columnPredicate = new hypertable.ColumnPredicate(args);

      var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval],
                                              column_predicates: [columnPredicate],
                                              columns: [ 'title' ],
                                              and_column_predicates: true});

      client.scanner_open(ns, 'products', scanSpec, function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testSecondaryValueIndexAndRowPrefix');
  });
};


var testAsyncSetup = function(callback) {
  console.log('[async]');
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlCreateProfileTable (callback) {
      client.hql_query(ns,
                       'CREATE TABLE Profile (info, last_access MAX_VERSIONS 1)',
                       callback);
    },
    function hqlPopulateProfileTable (callback) {
      client.hql_query(ns,
                       'INSERT INTO Profile VALUES ("1", "info:name", "Joe"), ' +
                       '("2", "info:name", "Sue")', callback);
    },
    function hqlCreateSessionTable (callback) {
      client.hql_query(ns, 'CREATE TABLE Session (user_id, page_hit)',
                       callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAsyncSetup');
  });
};


var testAsyncMutator = function(callback) {
  console.log('[async mutator]');
  var future;
  var asyncMutator = [];
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openFuture (callback) {
      client.future_open(0, function (error, result) {
          if (!error)
            future = result;
          callback(error);
        });
    },
    function openMutators (callback) {
      async.parallel([
        function openProfileMutator (callback) {
          client.async_mutator_open(ns, 'Profile', future, 0, callback);
        },
        function openSessionMutator (callback) {
          client.async_mutator_open(ns, 'Session', future, 0, callback);
        }
      ],
      function(error, results) {
        asyncMutator = results;
        callback(error);
      });
    },
    function asyncMutatorSetCells (callback) {
      async.parallel([
        function asyncMutatorProfileSetCells (callback) {
          var cells = [];
          var key = new hypertable.Key({row: '1', column_family: 'last_access'});
          var cell = new hypertable.Cell({key: key, value: '2014-06-13 16:06:09'});
          cells.push(cell);
          key = new hypertable.Key({row: '2', column_family: 'last_access'});
          cell = new hypertable.Cell({key: key, value: '2014-06-13 16:06:10'});
          cells.push(cell);
          client.async_mutator_set_cells(asyncMutator[0], cells, function (error) {
              if (!error)
                client.async_mutator_flush(asyncMutator[0], callback);
              else
                callback(error);
            });
        },
        function asyncMutatorSessionSetCells (callback) {
          var cells = [];
          var key = new hypertable.Key({row: '0001-200238',
                                        column_family: 'user_id',
                                        column_qualifier: '1'});
          var cell = new hypertable.Cell({key: key});
          cells.push(cell);
          key = new hypertable.Key({row: '0001-200238',
                                    column_family: 'page_hit'});
          cell = new hypertable.Cell({key: key, value: '/index.html'});
          cells.push(cell);
          key = new hypertable.Key({row: '0002-383049',
                                    column_family: 'user_id',
                                    column_qualifier: '2'});
          cell = new hypertable.Cell({key: key});
          cells.push(cell);
          key = new hypertable.Key({row: '0002-383049',
                                    column_family: 'page_hit'});
          cell = new hypertable.Cell({key: key, value: '/foo/bar.html'});
          cells.push(cell);
          client.async_mutator_set_cells(asyncMutator[1], cells, function (error) {
              if (!error)
                client.async_mutator_flush(asyncMutator[1], callback);
              else
                callback(error);
            });
        }
      ],
      function(error) { callback(error); });
    },
    function fetchResults (callback) {
      var resultCount = 0;
      var futureResult;
      async.doWhilst(
        function (callback) {
          client.future_get_result(future, 10000, function (error, result) {
              futureResult = result;
              if (!error && !futureResult.is_empty) {
                resultCount += 1;
                if (result.is_error)
                  error = new Error(result.error_msg);
                else if (result.id.toBuffer().compare(asyncMutator[0].toBuffer()))
                  console.log('Result is from Profile mutation');
                else if (result.id.toBuffer().compare(asyncMutator[1].toBuffer()))
                  console.log('Result is from Session mutation');
              }
              callback(error);
            });
        },
        function () { return !futureResult.is_empty; },
        function (error) {
          if (!error)
            console.log('result count = ' + resultCount);
          callback(error);
        }                     
      );
    },
    function closeMutators (callback) {
      async.parallel([
        function closeProfileMutator (callback) {
          client.async_mutator_close(asyncMutator[0], callback);
        },
        function closeSessionMutator (callback) {
          client.async_mutator_close(asyncMutator[1], callback);
        }
      ],
      function(error) { callback(error); });
    },
    function closeFuture (callback) {
      client.future_close(future, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAsyncMutator');
  });
};


var testAsyncScannerResult = function(callback) {
  console.log('[async scanner]');
  var future;
  var asyncScanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openFuture (callback) {
      client.future_open(0, function (error, result) {
          if (!error)
            future = result;
          callback(error);
        });
    },
    function openScanners (callback) {
      async.parallel([
        function openProfileScanner (callback) {
          var rowInterval = new hypertable.RowInterval({start_row: '1',
                                                        start_inclusive: true,
                                                        end_row: '1',
                                                        end_inclusive: true});
          var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval]});
          client.async_scanner_open(ns, 'Profile', future, scanSpec, callback);
        },
        function openSessionScanner (callback) {
          var rowInterval = new hypertable.RowInterval({start_row: '0001-200238',
                                                        start_inclusive: true,
                                                        end_row: '0001-200238',
                                                        end_inclusive: true});
          var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval]});
          client.async_scanner_open(ns, 'Session', future, scanSpec, callback);
        }
      ],
      function(error, results) {
        asyncScanner = results;
        callback(error);
      });
    },
    function fetchResults (callback) {
      var futureResult;
      async.doWhilst(
        function (callback) {
          client.future_get_result(future, 10000, function (error, result) {
              futureResult = result;
              if (!error && !futureResult.is_empty) {
                if (result.is_error) {
                  callback(new Error(result.error_msg));
                  return;
                }
                assert(futureResult.is_scan, 'Unexpected non-scanner result encountered');
                if (result.id.toBuffer().compare(asyncScanner[0].toBuffer()))
                  console.log('Result is from Profile scan');
                else if (result.id.toBuffer().compare(asyncScanner[1].toBuffer()))
                  console.log('Result is from Session scan');
                else {
                  callback(new Error('Invalid scanner ID encountered'));
                  return;
                }
                for (var j = 0; j < result.cells.length; j++)
                  console.log(result.cells[j].toString());
              }
              callback(error);
            });
        },
        function () { return !futureResult.is_empty; },
        function (error) { callback(error); }                     
      );
    },
    function closeScanners (callback) {
      async.parallel([
        function closeProfileScanner (callback) {
          client.async_scanner_close(asyncScanner[0], callback);
        },
        function closeSessionScanner (callback) {
          client.async_scanner_close(asyncScanner[1], callback);
        }
      ],
      function(error) { callback(error); });
    },
    function closeFuture (callback) {
      client.future_close(future, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAsyncScannerResult');
  });
};


var testAsyncScannerResultSerialized = function(callback) {
  console.log('[async scanner - result serialized]');
  var future;
  var asyncScanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openFuture (callback) {
      client.future_open(0, function (error, result) {
          if (!error)
            future = result;
          callback(error);
        });
    },
    function openScanners (callback) {
      async.parallel([
        function openProfileScanner (callback) {
          var rowInterval = new hypertable.RowInterval({start_row: '1',
                                                        start_inclusive: true,
                                                        end_row: '1',
                                                        end_inclusive: true});
          var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval]});
          client.async_scanner_open(ns, 'Profile', future, scanSpec, callback);
        },
        function openSessionScanner (callback) {
          var rowInterval = new hypertable.RowInterval({start_row: '0001-200238',
                                                        start_inclusive: true,
                                                        end_row: '0001-200238',
                                                        end_inclusive: true});
          var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval]});
          client.async_scanner_open(ns, 'Session', future, scanSpec, callback);
        }
      ],
      function(error, results) {
        asyncScanner = results;
        callback(error);
      });
    },
    function fetchResults (callback) {
      var futureResult;
      async.doWhilst(
        function (callback) {
          client.future_get_result_serialized(future, 10000, function (error, result_serialized) {
              futureResult = result_serialized;
              if (!error && !futureResult.is_empty) {
                if (result_serialized.is_error) {
                  callback(new Error(result_serialized.error_msg));
                  return;
                }
                assert(futureResult.is_scan, 'Unexpected non-scanner result encountered');
                if (result_serialized.id.toBuffer().compare(asyncScanner[0].toBuffer()))
                  console.log('Result is from Profile scan');
                else if (result_serialized.id.toBuffer().compare(asyncScanner[1].toBuffer()))
                  console.log('Result is from Session scan');
                else {
                  callback(new Error('Invalid scanner ID encountered'));
                  return;
                }
                var reader = new hypertable.SerializedCellsReader(result_serialized.cells);
                while (reader.next())
                  console.log(reader.getCell().toString());
              }
              callback(error);
            });
        },
        function () { return !futureResult.is_empty; },
        function (error) { callback(error); }                     
      );
    },
    function closeScanners (callback) {
      async.parallel([
        function closeProfileScanner (callback) {
          client.async_scanner_close(asyncScanner[0], callback);
        },
        function closeSessionScanner (callback) {
          client.async_scanner_close(asyncScanner[1], callback);
        }
      ],
      function(error) { callback(error); });
    },
    function closeFuture (callback) {
      client.future_close(future, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAsyncScannerResultSerialized');
  });
};


var testAsyncScannerResultAsArrays = function(callback) {
  console.log('[async scanner - result serialized]');
  var future;
  var asyncScanner;
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openFuture (callback) {
      client.future_open(0, function (error, result) {
          if (!error)
            future = result;
          callback(error);
        });
    },
    function openScanners (callback) {
      async.parallel([
        function openProfileScanner (callback) {
          var rowInterval = new hypertable.RowInterval({start_row: '1',
                                                        start_inclusive: true,
                                                        end_row: '1',
                                                        end_inclusive: true});
          var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval]});
          client.async_scanner_open(ns, 'Profile', future, scanSpec, callback);
        },
        function openSessionScanner (callback) {
          var rowInterval = new hypertable.RowInterval({start_row: '0001-200238',
                                                        start_inclusive: true,
                                                        end_row: '0001-200238',
                                                        end_inclusive: true});
          var scanSpec = new hypertable.ScanSpec({row_intervals: [rowInterval]});
          client.async_scanner_open(ns, 'Session', future, scanSpec, callback);
        }
      ],
      function(error, results) {
        asyncScanner = results;
        callback(error);
      });
    },
    function fetchResults (callback) {
      var futureResult;
      async.doWhilst(
        function (callback) {
          client.future_get_result_as_arrays(future, 10000, function (error, result_as_arrays) {
              futureResult = result_as_arrays;
              if (!error && !futureResult.is_empty) {
                if (result_as_arrays.is_error) {
                  callback(new Error(result_as_arrays.error_msg));
                  return;
                }
                assert(futureResult.is_scan, 'Unexpected non-scanner result encountered');
                if (result_as_arrays.id.toBuffer().compare(asyncScanner[0].toBuffer()))
                  console.log('Result is from Profile scan');
                else if (result_as_arrays.id.toBuffer().compare(asyncScanner[1].toBuffer()))
                  console.log('Result is from Session scan');
                else {
                  callback(new Error('Invalid scanner ID encountered'));
                  return;
                }
                for (var j = 0; j < result_as_arrays.cells.length; j++)
                  console.log(result_as_arrays.cells[j].slice(0 ,4));
              }
              callback(error);
            });
        },
        function () { return !futureResult.is_empty; },
        function (error) { callback(error); }                     
      );
    },
    function closeScanners (callback) {
      async.parallel([
        function closeProfileScanner (callback) {
          client.async_scanner_close(asyncScanner[0], callback);
        },
        function closeSessionScanner (callback) {
          client.async_scanner_close(asyncScanner[1], callback);
        }
      ],
      function(error) { callback(error); });
    },
    function closeFuture (callback) {
      client.future_close(future, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAsyncScannerResultAsArrays');
  });
};


var testAtomicCounterSetup = function(callback) {
  console.log('[atomic counter]');
  var ns;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlCreateTable (callback) {
      client.hql_query(ns, 'CREATE TABLE Hits (count COUNTER)', callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAtomicCounterSetup');
  });
};


var testAtomicCounterIncrement = function(callback) {
  console.log('[atomic counter - increment]');
  var mutator;
  var ns;
  var scanner;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openMutator (callback) {
      client.mutator_open(ns, "Hits", 0, 0, function (error, result) {
          if (!error)
            mutator = result;
          callback(error);
        });
    },
    function setCells (callback) {

      var cells = [];
      var key = new hypertable.Key({row: '/index.html',
                                    column_family: 'count',
                                    column_qualifier: '2014-06-14 07:31:18'});
      var cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:18'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/foo/bar.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:18'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/foo/bar.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:18'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/foo/bar.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:18'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/foo/bar.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '1'});
      cells.push(cell);
    
      client.mutator_set_cells(mutator, cells, function (error, result) {
          if (error)
            callback(error);
          else
            client.mutator_flush(mutator, callback);
        });
    },
    function openScanner (callback) {
      client.scanner_open(ns, 'Hits', new hypertable.ScanSpec(), function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeMutator (callback) {
      client.mutator_close(mutator, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAtomicCounterIncrement');
  });
};


var testAtomicCounterResetAndSubtraction = function(callback) {
  console.log('[atomic counter - reset and subtraction]');
  var mutator;
  var ns;
  var scanner;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function openMutator (callback) {
      client.mutator_open(ns, "Hits", 0, 0, function (error, result) {
          if (!error)
            mutator = result;
          callback(error);
        });
    },
    function setCells (callback) {

      var cells = [];
      var key = new hypertable.Key({row: '/index.html',
                                    column_family: 'count',
                                    column_qualifier: '2014-06-14 07:31:18'});
      var cell = new hypertable.Cell({key: key, value: '=0'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:18'});
      cell = new hypertable.Cell({key: key, value: '7'});
      cells.push(cell);

      key = new hypertable.Key({row: '/foo/bar.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:18'});
      cell = new hypertable.Cell({key: key, value: '-1'});
      cells.push(cell);

      key = new hypertable.Key({row: '/index.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '-2'});
      cells.push(cell);

      key = new hypertable.Key({row: '/foo/bar.html',
                                column_family: 'count',
                                column_qualifier: '2014-06-14 07:31:19'});
      cell = new hypertable.Cell({key: key, value: '=19'});
      cells.push(cell);
    
      client.mutator_set_cells(mutator, cells, function (error, result) {
          if (error)
            callback(error);
          else
            client.mutator_flush(mutator, callback);
        });
    },
    function openScanner (callback) {
      client.scanner_open(ns, 'Hits', new hypertable.ScanSpec(), function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeMutator (callback) {
      client.mutator_close(mutator, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testAtomicCounterResetAndSubtraction');
  });
};


var testUnique = function(callback) {
  console.log('[unique]');
  var ns;
  var scanner;
  async.series([
    function openTestNamespace (callback) {
      client.namespace_open('test', function (error, result) {
          if (!error)
            ns = result;
          callback(error);
        });
    },
    function hqlCreateTable (callback) {
      client.hql_query(ns, 'CREATE TABLE User (info, id TIME_ORDER desc MAX_VERSIONS 1)', callback);
    },
    function createCellsUnique (callback) {
      async.parallel([
        function (callback) {
          var key = new hypertable.Key({row: 'joe1987', column_family: 'id'});
          client.create_cell_unique(ns, 'User', key, '', callback);
        },
        function (callback) {
          var key = new hypertable.Key({row: 'mary.bellweather', column_family: 'id'});
          client.create_cell_unique(ns, 'User', key, '', callback);
        }
      ],
      function(error) { callback(error); });
    },
    function createConflictingCell (callback) {
      var key = new hypertable.Key({row: 'joe1987', column_family: 'id'});
      client.create_cell_unique(ns, 'User', key, '', function (error, result) {
          if (error && error.code == 48) {
            console.log('User name "' + key.row + '" is already taken');
            callback();
          }
          else
            callback(error);
        });
    },
    function openScanner (callback) {
      client.scanner_open(ns, 'User', new hypertable.ScanSpec(), function (error, result) {
          if (!error)
            scanner = result;
          callback(error);
        });
    },
    function scannerGetCells (callback) {
      var cells;
      async.doWhilst(
        function (callback) {
          client.scanner_get_cells(scanner, function (error, result) {
              if (!error) {
                cells = result;
                for (var j=0; j < cells.length; j++)
                  console.log(cells[j].toString());                  
              }
              callback(error);
            });
        },
        function () { return Boolean(cells.length); },
        function (error) {
          callback(error);
        }                     
      );
    },
    function closeScanner (callback) {
      client.scanner_close(scanner, callback);
    },
    function closeTestNamespace (callback) {
      client.namespace_close(ns, callback);
    }
  ],
  function(error) {
    callback(error, 'testUnique');
  });
};


async.series([
  testBasic,
  testConvenience,
  testCreateTable,
  testAlterTable,
  testMutator,
  testScannerFullTable,
  testScannerRestricted,
  testHql,
  testHqlAsArrays,
  testHqlExecMutator,
  testHqlExecScanner,
  testSecondaryIndexSetup,
  testSecondaryValueIndexExactMatch,
  testSecondaryValueIndexExactMatchWithQualifier,
  testSecondaryValueIndexPrefixMatch,
  testSecondaryValueIndexRegexMatch,
  testSecondaryQualifierIndexExists,
  testSecondaryQualifierIndexExistsRegexPrefixMatch,
  testSecondaryValueIndexOrQuery,
  testSecondaryValueIndexAndQuery,
  testSecondaryValueIndexAndRowInterval,
  testSecondaryValueIndexAndRowPrefix,
  testAsyncSetup,
  testAsyncMutator,
  testAsyncScannerResult,
  testAsyncScannerResultSerialized,
  testAsyncScannerResultAsArrays,
  testAtomicCounterSetup,
  testAtomicCounterIncrement,
  testAtomicCounterResetAndSubtraction,
  testUnique
  ],
  function(error, results) {
    client.closeConnection();
    if (error) {
      console.log(util.format('%s: %s', error.name, error.message));
      process.exit(1);
    }
  }
);
