'use strict';

var utils = require('./utils');
var currentMicros = utils.currentMicros;
var metrics = require('metrics');
var perfmetrics = require('../perfmetrics');
var util = require('util');
var LinkedList = require('linkedlist');

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
    // Error Counter
    var errorCounterInc = 0;
    // Request counter
    var requestCounterInc = 0;
    // Request latencies
    var requestLatenciesList = new LinkedList(); //Using LinkedList for fast push
    // Request latencies snapshot
    var requestTimerSnapshotList = new LinkedList(); //Using LinkedList for fast push
    // Definition of the metrics name path at graphite server
    // graphite metrics name: nodejs.<driver version>.<workload>.<outstanding requests>
    var reportPrefix = util.format('drivers.nodejs.%s.%s.%s',
              options.graphitePrefix.replace(new RegExp('\\.', 'g'), '_'),
              self._name,
              options.outstanding);
    console.log('---  %s / %s  ---', self._name, item.name);
    var takeSnapshot = function() {
      requestTimerSnapshotList.push({
        timestamp: (new Date).getTime(), 
        counter: requestCounterInc,
        errCounter : errorCounterInc
      });
    }

    var handler = function latencyTrackerHandler(n, timesNext) {
      var queryStart = currentMicros();
      item.fn(self._client, n, function (err) {
        var duration = currentMicros() - queryStart;
        requestLatenciesList.push(duration);
        requestCounterInc++;
        if (err) {
          errorCounterInc++;
        }
        timesNext(err);
      });
    };
    
    var snapshotInterval;
    // Record the snapshot of metrics every 250ms to later report.
    // Start to take snapshots after 250ms of starting tests.
    // This frequency must the careful chosen to no impact too much on results
    setTimeout(function() {
      snapshotInterval = setInterval(takeSnapshot, 250);
    }, 500);

    var testStart = (new Date).getTime();
    utils.timesLimit(options.ops, options.outstanding, handler, function (err) {
      takeSnapshot();
      clearInterval(snapshotInterval);
      var testStop = (new Date).getTime();
      self._logMessage('Finished run: %s', item._name);
      var metricsReport = new metrics.Report();
      var requestTimer = new perfmetrics.Timer(testStart, testStop);
      var errMeter = new perfmetrics.Meter(testStart, testStop);
      metricsReport.addMetric(item.name + '.requests', requestTimer);
      metricsReport.addMetric(item.name + '.errors', errMeter);
      self._logMessage('Created metrics');
      // Use a local memory reporter to not let the communication with graphite server
      // impact on test results : throughput
      var localMemReporter = new perfmetrics.LocalMemReporter(metricsReport, reportPrefix, baseTime);
      var lastIndex = 0;
      while (requestTimerSnapshotList.length) {
        var snapshot = requestTimerSnapshotList.shift();
        var timestamp = snapshot.timestamp;
        var counter = snapshot.counter;
        var errCounter = snapshot.errCounter;
        var latenciesCounterIndex = counter;
        for(var k = lastIndex; k < latenciesCounterIndex; k++) {
          requestTimer.update(requestLatenciesList.shift());
        }
        lastIndex = latenciesCounterIndex;
        requestTimer.setStopTime(timestamp);
        if (errCounter > 0) {
          errMeter.mark(errCounter);
        }
        if (options.graphiteHost) {
          localMemReporter.report(timestamp / 1000);
        }
      }
      localMemReporter.reportConsole(timestamp);
      if (options.graphiteHost) {
        localMemReporter.reportGraphite(options.graphiteHost, options.graphitePort, function() {
          self._logMessage('Successfully reported to graphite');
          next(null);
        });
      } else {
        next(err);
      }
    });
  }, function(err) {
    console.log('finished all workloads, using basetime: %s', baseTime);
    callback(err);
  });
};

module.exports = ClientWorkload;