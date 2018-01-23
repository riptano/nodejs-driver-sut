'use strict';

var utils = require('./utils');
var currentMicros = utils.currentMicros;
var metrics = require('metrics');
var perfmetrics = require('../perfmetrics');
var util = require('util');
var LinkedList = require('linkedlist');
const Histogram = require('native-hdr-histogram');

var warmupLength = 256;

/**
 * @classdesc
 * Represents a benchmark workload that uses a <code>Client</code> instance.
 * @param {String} name
 * @constructor
 */
function ClientWorkload(name) {
  this._name = name;
  /**
   * @type {Array<Function>}
   * @private
   */
  this._before = [];
  /**
   * @type {Array<{name, fn}>}
   * @private
   */
  this._items = [];
  /**
   * @type {Array<String>}
   * @private
   */
  this._setupQueries = [];
  this._debug = false;
  this._commandLineOptions = utils.parseCommonOptions();
}

ClientWorkload.prototype.before = function (fn) {
  this._before.push(fn);
  return this;
};

ClientWorkload.prototype.queries = function (queries) {
  this._setupQueries.push.apply(this._setupQueries, queries);
  return this;
};

ClientWorkload.prototype.add = function (name, fn) {
  this._items.push({ name: name, fn: fn });
  return this;
};

/**
 * Starts running the workload.
 * Once it finishes it will shutdown the client.
 * @param {Object} [options]
 * @param {Boolean} [options.debug]
 * @param {Function} [callback]
 */
ClientWorkload.prototype.run = function (options, callback) {
  options = options || {};
  this._debug = options.debug;
  utils.outputTestHeader(this._commandLineOptions);
  // eslint-disable-next-line global-require
  var Client = require(this._commandLineOptions.driverPackageName).Client;
  this._client = new Client(utils.connectOptions());
  var self = this;
  utils.series([
    this._client.connect.bind(this._client),
    this._setup.bind(this),
    // this._warmup.bind(this),
    this._runWorkloadItems.bind(this)
  ], function (err) {
    self._client.shutdown(function () {
      if (callback) {
        return callback(err);
      }
      if (err) {
        throw err;
      }
    });
  });
};

ClientWorkload.prototype._logMessage = function () {
  if (!this._debug) {
    return;
  }
  console.log.apply(null, arguments);
};

ClientWorkload.prototype._setup = function (callback) {
  var self = this;
  this._logMessage('Executing setup');
  utils.series([
    function runBefore(next) {
      utils.eachSeries(self._before, function (beforeFn, eachNext) {
        beforeFn(self._client, eachNext);
      }, next);
    },
    function executeSetupQueries(next) {
      utils.eachSeries(self._setupQueries, function (query, eachNext) {
        self._client.execute(query, eachNext);
      }, next);
    }
  ], callback);
};

ClientWorkload.prototype._warmup = function (callback) {
  var client = this._client;
  this._logMessage('Warming up the %d workload items by running %d times each', this._items.length, warmupLength);
  utils.eachSeries(this._items, function (item, next) {
    utils.timesLimit(warmupLength, 8, function (n, timesNext) {
      item.fn(client, n, timesNext);
    }, next);
  }, callback);
};

ClientWorkload.prototype._runWorkloadItems = function (callback) {
  var options = this._commandLineOptions;
  this._logMessage('Running %d workload items', this._items.length);
  var self = this;
  // It was included the concept of basetime to report a test run using
  // a basetime as timestamp. It is useful to generate results on graphite
  var currentTime = (new Date()).getTime();
  var baseTime = options.basetime || currentTime;
  utils.eachSeries(this._items, function (item, next) {
    const histogram = new Histogram(1, 500);
    // Error Counter
    var errorCounterInc = 0;
    // Request counter
    var requestCounterInc = 0;
    console.log('---  %s / %s  ---', self._name, item.name);
    var handler = function latencyTrackerHandler(n, timesNext) {
      var queryStart = currentMicros();
      item.fn(self._client, n, function (err) {
        var duration = currentMicros() - queryStart;
        histogram.record(duration);
        requestCounterInc++;
        if (err) {
          errorCounterInc++;
        }
        timesNext(err);
      });
    };
    
    var iteratorFunction = utils.timesLimit;
    var duration = options.ops;
    if (options.seconds) {
      iteratorFunction = utils.timeInSecondsLimit;
      duration = options.seconds;
    }
    if (options.rate === 'fixed') {
      iteratorFunction = utils.timeInSecondsFixedRate;
      duration = options.seconds;
    }

    var localMemRecorder = new perfmetrics.LocalMemRecorder(baseTime);
    // Record the snapshot of metrics every 500ms to later report.
    // This frequency must the careful chosen to no impact too much on results
    var takeSnapshot = function () {
      localMemRecorder.record(requestCounterInc, errorCounterInc, histogram);
    };
    //as graphite store the data using seconds since unix epoch time, do not make sense to retrieve a lower interval
    var testStart = localMemRecorder.start(takeSnapshot, 1000);
    iteratorFunction(duration, options.outstanding, handler, function (err) {
      takeSnapshot();
      localMemRecorder.stop();
      localMemRecorder.reportConsole();
      if (options.graphiteHost) {
        // Definition of the metrics name path at graphite server
        // graphite metrics name: drivers.nodejs.<driverpackagename>.<driver version>.<workload>.<requesttype>.<db>.<nodes>.<rate>.<outstanding requests>
        var reportPrefix = util.format('drivers.nodejs.%s.%s.%s.%s.%s.%s.%s.%s',
                  (options.driverPackageName === 'cassandra-driver' ? 'oss' : 'dse'),
                  options.driverVersion.replace(new RegExp('\\.', 'g'), '_'),
                  self._name,
                  item.name,
                  options.database.replace(new RegExp('\\.', 'g'), '_'),
                  options.nodes,
                  options.rate,
                  options.outstanding);
        console.log('prefix: ' + reportPrefix);
        localMemRecorder.reportGraphite(options.graphiteHost, options.graphitePort, reportPrefix, function() {
          self._logMessage('Successfully reported to graphite');
          next(null);
        });
      } else {
        next(null);
      }
    });
  }, function(err) {
    console.log('finished all workloads, using basetime: %s', baseTime);
    callback(err);
  });
};

module.exports = ClientWorkload;