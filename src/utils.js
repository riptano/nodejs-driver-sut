'use strict';
var fs = require('fs');
var Promise = require('bluebird');
var cassandra = require('cassandra-driver');
const sprintf = require('sprintf-js').sprintf;

function noop() {}
var logItemFormat = "%7.2f,%7.2f,%7.2f,%7.2f,%7.2f,%7.2f,%7.2f,%7.2f,%7.2f,%7.2f,%9.2f,%9.2f,%9.2f,%9.2f";
var logHeaderFormat = "    min,     25,     50,     75,     95,     98,     99,   99.9,    max,   mean,    thrpt,      rss,heapTotal, heapUsed";

exports.times = function times (n, f){
  var arr = new Array(n);
  for (var i = 0; i < n; i++) {
    arr[i] = f();
  }
  return arr;
};

/**
 * @param {Number} count
 * @param {Function} iteratorFunc
 * @param {Function} [callback]
 */
exports.aTimes = function (count, iteratorFunc, callback) {
  callback = callback || noop;
  count = +count;
  if (isNaN(count) || count === 0) {
    return callback();
  }
  var completed = 0;
  for (var i = 0; i < count; i++) {
    iteratorFunc(i, next);
  }
  function next(err) {
    if (err) {
      var cb = callback;
      callback = noop;
      return cb(err);
    }
    if (++completed !== count) {
      return;
    }
    callback();
  }
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
    var defaultTypeName = typeof defaults[name];
    if (defaultTypeName === 'number') {
      // use the same type
      options[name] = parseFloat(options[name]);
    }
    else if (defaultTypeName === 'boolean') {
      options[name] = (options[name] === 'true');
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
  var options = parseOptions({
    'c':  ['contactPoint', 'Cassandra contact point'],
    'ks': ['keyspace', 'Keyspace name'],
    'p':  ['connectionsPerHost', 'Number of connections per host'],
    'r':  ['ops', 'Number of requests per series'],
    's':  ['series', 'Number of series'],
    'o':  ['outstanding', 'Maximum amount of outstanding requests'],
    't':  ['throttle', 'Maximum amount of requests to allow per second'],
    'f':  ['promiseFactoryName', 'Promise factory to use, options: [\'default\', \'bluebird\', \'q\']'],
    'l':  [
      'measureLatency', 'Determines it should measure latencies, options: [\'true\', \'false\'].' +
      ' Default:\'false\''],
    'd':  ['driverPackageName', 'The name of the driver package: [\'cassandra-driver\', \'dse-driver\']'],
    'h':  ['help', 'Displays the help']
  }, extend({
    outstanding: 256,
    connectionsPerHost: 1,
    ops: 100000,
    series: 10,
    throttle: 1000000,
    promiseFactoryName: 'default',
    measureLatency: false,
    driverPackageName: 'cassandra-driver'
  }, defaults));

  if (options.promiseFactoryName === 'bluebird') {
    // eslint-disable-next-line global-require
    options.promiseFactory = require('bluebird').fromCallback;
  } else if (options.promiseFactoryName === 'q') {
    // wrap Q.nfcall.
    // eslint-disable-next-line global-require
    var Q = require(options.promiseFactoryName);
    options.promiseFactory = function qFactory(handlerWrapper) {
      return Q.nfcall(handlerWrapper);
    };
  } else if (options.promiseFactoryName !== 'default') {
    console.error("Got unknown promise factory for option '-f': %s", options.promiseFactoryName);
    process.exit(-1);
  }
  return options;
};

/**
 * Returns options to be passed to Client based on result of [parseCommonOptions].
 */
exports.connectOptions = function connectOptions() {
  var options = this.parseCommonOptions();
  return {
    contactPoints: [ options.contactPoint || '127.0.0.1' ],
    localDataCenter: 'dc1',
    policies: { loadBalancing: new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy()},
    socketOptions: { tcpNoDelay: true },
    pooling: {
      coreConnectionsPerHost: {'0': options.connectionsPerHost, '1': 1, '2': 0},
      heartBeatInterval: 30000
    }
  };
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
    // eslint-disable-next-line global-require
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
  console.log('- Max requests per second: %d', options.throttle);
  console.log('- Operations per series: %d', options.ops);
  console.log('- Series count: %d', options.series);
  console.log('- Measure latency: %s', options.measureLatency);
  console.log('- Promise factory: %s', options.promiseFactoryName);
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


exports.timesPerSec = function (count, limit, perSec, iteratorFunc, onInterval, callback) {
  callback = callback || noop;
  limit = Math.min(limit, count);
  var index = limit - 1;
  var completed = 0;
  let queued = true;
  let queuedIndex = index;
  let queuedCount = 0;
  let ceilOnInterval = Math.min(limit, perSec);
  let submittedInSecond = ceilOnInterval;
  for (var i = 0; i < ceilOnInterval; i++) {
    iteratorFunc(i, next);
  }

  let done = false;
  let finalErr = null;

  const interval = setInterval(() => {
    onInterval();
    if (done) {
      clearInterval(interval);
      callback(finalErr);
    }
    let toSubmit;
    if (queuedCount == 0) {
      submittedInSecond = 0;
      return;
    }

    if (queuedCount > ceilOnInterval) {
      toSubmit = ceilOnInterval;
      queuedCount -= ceilOnInterval;
    } else {
      toSubmit = queuedCount;
      queuedCount = 0;
      queued = false;
    }

    let curIndex = queuedIndex;
    submittedInSecond = toSubmit;
    for (let i = 0; i < toSubmit; i++) {
      iteratorFunc(curIndex++, next);
    }
    queuedIndex = curIndex;
  }, 1000);

  function next(err) {
    if (err) {
      finalErr = err;
      done = true;
      return;
    }
    if (++completed === count) {
      done = true;
      return;
    }
    if (++index >= count) {
      done = true;
      return;
    }

    if (submittedInSecond < perSec) {
      submittedInSecond += 1;
      iteratorFunc(index, next);
      return;
    }

    if (!queued) {
      queued = true;
      queuedIndex = index;
    }
    queuedCount++;
    return;
  }
};

/**
 * Similar to async.series(), but instead accumulating the result in an Array, it callbacks with the result of the last
 * function in the array.
 * @param {Array.<Function>} arr
 * @param {Function} [callback]
 */
exports.series = function (arr, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter must be an Array');
  }
  callback = callback || noop;
  var index = 0;
  var sync;
  next();
  function next(err, result) {
    if (err) {
      return callback(err);
    }
    if (index === arr.length) {
      return callback(null, result);
    }
    if (sync) {
      return process.nextTick(function () {
        //noinspection JSUnusedAssignment
        sync = true;
        arr[index++](next);
        sync = false;
      });
    }
    sync = true;
    arr[index++](next);
    sync = false;
  }
};

/**
 * @param {Array} arr
 * @param {Function} fn
 * @param {Function} [callback]
 */
exports.eachSeries = function eachSeries(arr, fn, callback) {
  if (!Array.isArray(arr)) {
    throw new TypeError('First parameter is not an Array');
  }
  callback = callback || noop;
  var length = arr.length;
  if (length === 0) {
    return callback();
  }
  var sync;
  var index = 1;
  fn(arr[0], next);
  if (sync === undefined) {
    sync = false;
  }

  function next(err) {
    if (err) {
      return callback(err);
    }
    if (index >= length) {
      return callback();
    }
    if (sync === undefined) {
      sync = true;
    }
    if (sync) {
      return process.nextTick(function () {
        fn(arr[index++], next);
      });
    }
    fn(arr[index++], next);
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
  console.log(logHeaderFormat);
};

exports.logTimer = function (timer, millis, start, count) {
  var mem;
  let meanRate = count;
  // if start or elapsed is set calculate rate. 
  // otherwise assume count encompasses responses in 1 second interval.
  if (millis || start) {
    if (millis === null) {
      const elapsed = process.hrtime(start);
      millis = elapsed[0] * 1000 + elapsed[1] / 1000000;
    }
    meanRate = count * 1000 / millis;
  }
  if (!timer) {
    // Use process.hrtime()
    mem = process.memoryUsage();
    console.log(sprintf(logItemFormat,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      meanRate.toFixed(2),
      (mem.rss / 1024.0 / 1024.0).toFixed(2),
      (mem.heapTotal / 1024.0 / 1024.0).toFixed(2),
      (mem.heapUsed / 1024.0 / 1024.0).toFixed(2)));
      return;
  }

  var percentiles = timer.percentiles([0.25,0.50,0.75,0.95,0.98,0.99,0.999]);
  mem = process.memoryUsage();
  console.log(sprintf(logItemFormat,
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
    meanRate.toFixed(2),
    (mem.rss / 1024.0 / 1024.0).toFixed(2),
    (mem.heapTotal / 1024.0 / 1024.0).toFixed(2),
    (mem.heapUsed / 1024.0 / 1024.0).toFixed(2)));
};

// Logs a final summary with the given timer.
exports.logTotals = function (totalTimer, elapsed, count) {
  process.stdout.write('\n');
  console.log("Totals:");
  this.logTimerHeader();
  if (totalTimer) {
    this.logTimer(totalTimer, elapsed, null, count);
  }
  else {
    this.logTimer(null, elapsed, null, count);
  }
  console.log();
  console.log("Operations: ", count);
  console.log('-------------------------');
};

// returns an array from 0..N-1.
var timesIt = function(count) {
  var d = [];
  for (var i = 0; i < count; i++) {
    d.push(i);
  }
  return d;
};

// Executes fn n times concurrently to produce promises and then returns a single Promise on the result of all promises.
exports.pTimes = function (n, fn) {
  return Promise.map(timesIt(n), function (i) { return fn(i); });
};

// Executes fn n times with concurrency of limit to produce promises and then returns a single Promise on the result of all promises.
exports.pTimesLimit = function (n, limit, fn) {
  return Promise.map(timesIt(n), function(i) { return fn(i); }, {concurrency: limit});
};

// Executes fn n times one at a time to produce promises and then returns a signle Promise on the result of all promises.
exports.pTimesSeries = function (n, fn) {
  return this.pTimesLimit(n, 1, fn);
};

/**
 * Initializes the killrvideo keyspace.
 * 
 * @param {Object} connectOptions options to connect with.
 * @param {String} keyspace keyspace to initialize.
 * @param {Function} callback to execute with on completion of schema init.
 */
exports.initSchema = function(connectOptions, keyspace, callback) {
  var client = new cassandra.Client(connectOptions);
  this.series([
    client.connect.bind(client),
    function (next) {
      client.execute("DROP KEYSPACE IF EXISTS " + keyspace, next);
    },
    function (next) {
      client.execute("CREATE KEYSPACE " + keyspace + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", next);
    },
    function (next) {
      client.execute("CREATE TABLE " + keyspace + ".comments_by_video (videoid uuid, commentid timeuuid, userid uuid, comment text, PRIMARY KEY (videoid, commentid)) WITH CLUSTERING ORDER BY (commentid DESC)", next);
    },
    client.shutdown.bind(client)
  ], callback);
};
