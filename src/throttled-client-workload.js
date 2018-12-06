'use strict';

var utils = require('./utils');
var currentMicros = utils.currentMicros;
var metrics = require('metrics');

var warmupLength = 50000;

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
  let outstandingTargets = [];
  if (options.outstandingBase > 0) {
    for (let i = options.outstandingBase; i < options.outstanding; i+= options.outstandingStep) {
      outstandingTargets.push(i);
    }
  }
  if (outstandingTargets.length == 0 || outstandingTargets[outstandingTargets.length - 1] != options.outstanding) {
    outstandingTargets.push(options.outstanding);
  }

  let outstandingPerName = {};
  this._items.forEach((item) => outstandingPerName[item.name] = []);

  utils.eachSeries(outstandingTargets, (outstanding, stepNext) => {
    utils.eachSeries(this._items, (item, next) => {
      let totalTimer;
      console.log('---  %s / %s @ %d outstanding ---', this._name, item.name, outstanding);
      if (this._commandLineOptions.measureLatency) {
        totalTimer = new metrics.Timer();
      }
      utils.logTimerHeader();
      let handler;
      let seriesTimer;
      let intervalCount = 0;
      if (this._commandLineOptions.measureLatency) {
        seriesTimer = new metrics.Timer();
        handler = (n, timesNext) => {
          var queryStart = currentMicros();
          item.fn(this._client, n, function (err) {
            var duration = currentMicros() - queryStart;
            seriesTimer.update(duration);
            totalTimer.update(duration);
            timesNext(err);
          });
        };
      }
      else {
        handler = (n, timesNext) => {
          item.fn(this._client, n, function (err) {
            intervalCount++;
            timesNext(err);
          });
        };
      }

      let lastTotalCount = 0;
      let start = process.hrtime();
      const onInterval = () => {
        if (totalTimer) {
          intervalCount = totalTimer.count() - lastTotalCount;
          lastTotalCount = totalTimer.count();
        }
        utils.logTimer(seriesTimer, null, null, intervalCount);
        intervalCount = 0;
        if (seriesTimer) {
          seriesTimer.clear();
        }
      };

      utils.timesPerSec(options.ops, outstanding, options.throttle, handler, onInterval, (err) => {
        const elapsed = process.hrtime(start);
        const millis = elapsed[0] * 1000 + elapsed[1] / 1000000;
        utils.logTotals(totalTimer, millis, options.ops);
        outstandingPerName[item.name].push({
          target: outstanding,
          elapsed: millis,
          timer: totalTimer
        });
        next(err);
      });
    }, (err) => {
      stepNext(err);
    });
  }, (err) => {
    console.log('Final Summary:');
    Object.keys(outstandingPerName).forEach((name) => {
      console.log('--- %s ---', name);
      utils.logTimerHeaderForTarget();
      let outstanding = outstandingPerName[name];
      outstanding.forEach((t) => {
        utils.logTimerForTarget(t.target, t.timer, t.elapsed, null, options.ops);
      });
    });
    callback(err);
  });
};

module.exports = ClientWorkload;
