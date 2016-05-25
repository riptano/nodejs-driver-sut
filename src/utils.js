'use strict';
var fs = require('fs');

exports.times = function times (n, f){
  var arr = new Array(n);
  for (var i = 0; i < n; i++) {
    arr[i] = f();
  }
  return arr;
};

exports.mean = function mean(arr) {
  return (arr.reduce(function (p, v) { return p + v; }, 0) / arr.length);
};

exports.median = function median(arr) {
  arr = arr.slice(0).sort();
  var num = arr.length;
  if (num % 2 !== 0) {
    return arr[(num - 1) / 2];
  }
  // even: return the average of the two middle values
  var left = arr[num / 2 - 1];
  var right = arr[num / 2];

  return (left + right) / 2;
};

exports.min = function min(arr) {
  if (!arr || arr.length === 0) {
    throw new Error('No elements');
  }
  return arr.reduce(function (prev, curr) {
    if (curr < prev) {
      return curr;
    }
    return prev;
  }, 9007199254740991);
};

var parseOptions = exports.parseOptions = function parseOptions(optionNames, defaults) {
  if (process.argv.indexOf('-h') > 0 || process.argv.indexOf('--help') > 0){
    // print options
    console.log('Usage:\n\tnode <file_name> <options>');
    console.log('Where options can be:');
    Object.keys(optionNames).forEach(function (name) {
      var values = optionNames[name];
      console.log('\t-%s <%s>\n\t\t%s', name, values[0].toUpperCase(), values[1] || '');
    });
    return process.exit();
  }
  var options = {};
  for (var i = 0; i < process.argv.length; i = i + 2) {
    var optionId = process.argv[i];
    if (!optionId || optionId.indexOf('-') !== 0) {
      continue;
    }
    optionId = optionId.substr(1);
    var name = optionId;
    if (optionNames[optionId]) {
      name = optionNames[optionId][0] || optionId;
    }
    options[name] = process.argv[i + 1];
  }
  Object.keys(defaults).forEach(function (name) {
    options[name] = options[name] || defaults[name];
    if (typeof defaults[name] === 'number') {
      // use the same type
      options[name] = parseFloat(options[name]);
    }
  });
  return options;
};

/**
 * @param defaults
 * @returns {{contactPoint: String|undefined, keyspace: String|undefined, outstanding: Number, ops: Number,
 *  series: Number, connectionsPerHost: Number}}
 */
exports.parseCommonOptions = function parseCommonOptions(defaults) {
  return parseOptions({
    'c':  ['contactPoint', 'Cassandra contact point'],
    'ks': ['keyspace', 'Keyspace name'],
    'p':  ['connectionsPerHost', 'Number of connections per host'],
    'r':  ['ops', 'Number of requests per series'],
    's':  ['series', 'Number of series'],
    'o':  ['outstanding', 'Maximum amount of outstanding requests'],
    'h':  ['help', 'Displays the help']
  }, extend({
    outstanding: 256,
    connectionsPerHost: 1,
    ops: 100000,
    series: 10
  }, defaults));
};

/**
 * Merge the contents of two or more objects together into the first object. Similar to jQuery.extend
 */
var extend = exports.extend = function (target) {
  var sources = Array.prototype.slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
};

exports.requireOptional = function (moduleName) {
  var result;
  try{
    result = require(moduleName);
  }
  catch (e) {
    console.error('Module %s not found', moduleName);
  }
  return result;
};

exports.currentMicros = function() {
  var t = process.hrtime();
  return t[0] * 1000 + t[1] / 1000000;
};

/**
 * @param options
 */
exports.outputTestHeader = function outputTestHeader(options) {
  console.log('-----------------------------------------------------');
  console.log('Using:');
  var driverVersion = JSON.parse(fs.readFileSync('node_modules/cassandra-driver/package.json', 'utf8')).version;
  console.log('- Driver v%s', driverVersion);
  console.log('- Connections per hosts: %d', options.connectionsPerHost);
  console.log('- Max outstanding requests: %d', options.outstanding);
  console.log('- Operations per series: %d', options.ops);
  console.log('- Series count: %d', options.series);
  console.log('-----------------------------------------------------');
};

/**
 * @param {Number} count
 * @param {Number} limit
 * @param {Function} iteratorFunc
 * @param {Function} [callback]
 */
exports.timesLimit = function (count, limit, iteratorFunc, callback) {
  callback = callback || noop;
  limit = Math.min(limit, count);
  var index = limit - 1;
  var completed = 0;
  for (var i = 0; i < limit; i++) {
    iteratorFunc(i, next);
  }
  function next(err) {
    if (err) {
      var cb = callback;
      callback = noop;
      cb(err);
      return;
    }
    if (++completed === count) {
      return callback();
    }
    if (++index >= count) {
      return;
    }
    iteratorFunc(index, next);
  }
};

/**
 * @param {Number} count
 * @param {Function} iteratorFunction
 * @param {Function} callback
 */
exports.timesSeries = function timesSeries(count, iteratorFunction, callback) {
  count = +count;
  if (isNaN(count) || count < 1) {
    return callback();
  }
  var index = 0;
  iteratorFunction(index, next);

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (++index === count) {
      return callback();
    }
    iteratorFunction(index, next);
  }
};

exports.logTimerHeader = function () {
  console.log("min,25,50,75,95,98,99,99.9,max,mean,count,thrpt,rss,heapTotal,heapUsed");
};

exports.logTimer = function (timer) {
  var percentiles = timer.percentiles([.25,.50,.75,.95,.98,.99,.999]);
  var mem = process.memoryUsage();
  console.log("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
    timer.min().toFixed(2),
    percentiles['0.25'].toFixed(2),
    percentiles['0.5'].toFixed(2),
    percentiles['0.75'].toFixed(2),
    percentiles['0.95'].toFixed(2),
    percentiles['0.98'].toFixed(2),
    percentiles['0.99'].toFixed(2),
    percentiles['0.999'].toFixed(2),
    timer.max().toFixed(2),
    timer.mean().toFixed(2),
    timer.count(),
    timer.meanRate().toFixed(2),
    (mem.rss / 1024.0 / 1024.0).toFixed(2),
    (mem.heapTotal / 1024.0 / 1024.0).toFixed(2),
    (mem.heapUsed / 1024.0 / 1024.0).toFixed(2));
};