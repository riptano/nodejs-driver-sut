'use strict';

const cluster = require('cluster');

const { WorkersThroughputBenchmark } = require('./lib/benchmarks');

if (cluster.isMaster) {
  WorkersThroughputBenchmark
    .createMaster()
    .run();

} else {
  WorkersThroughputBenchmark
    .createWorker()
    .run();
}