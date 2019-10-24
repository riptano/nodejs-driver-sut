'use strict';

const { ThroughputBenchmark } = require('./lib/benchmarks');

ThroughputBenchmark
  .create()
  .run();

