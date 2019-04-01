'use strict';

var utils = require('./utils');
var currentMicros = utils.currentMicros;
var metrics = require('metrics');

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

  this._client.on('log', (level, className, message) => {
    if (level !== 'verbose') {
      console.log(level, className, message);
    }
  });

  var self = this;
  utils.series([
    this._client.connect.bind(this._client),
    this._setup.bind(this),
    this._warmup.bind(this),
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
  this._logMessage('Running %d workload items');
  var options = this._commandLineOptions;
  var self = this;
  utils.eachSeries(this._items, function (item, next) {
    var totalTimer;
    console.log('---  %s / %s  ---', self._name, item.name);
    if (self._commandLineOptions.measureLatency) {
      totalTimer = new metrics.Timer();
    }
    utils.logTimerHeader();
    var elapsed = [];
    utils.timesSeries(options.series, function (n, nextIteration) {
      var handler;
      var seriesTimer;
      var start;
      if (self._commandLineOptions.measureLatency) {
        seriesTimer = new metrics.Timer();
        handler = function latencyTrackerHandler(n, timesNext) {
          var queryStart = currentMicros();
          item.fn(self._client, n, function (err) {
            var duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
            timesNext(err);
          });
        };
      }
      else {
        start = process.hrtime();
        handler = function latencyTrackerHandler(n, timesNext) {
          item.fn(self._client, n, timesNext);
        };
      }
      utils.timesLimit(options.ops, options.outstanding, handler, function (err) {
        elapsed.push(utils.logTimer(seriesTimer, null, start, options.ops));
        nextIteration(err);
      });
    }, function (err) {
      utils.logTotals(totalTimer, elapsed, options.ops * options.series);
      next(err);
    });
  }, callback);
};

module.exports = ClientWorkload;