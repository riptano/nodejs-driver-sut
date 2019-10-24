'use strict';

const ThroughputBenchmark = require('./throughput');
const WorkersThroughputBenchmark = require('./workers-throughput');
const LatencyBenchmark = require('./latency');

module.exports = {
  ThroughputBenchmark,
  WorkersThroughputBenchmark,
  LatencyBenchmark
};