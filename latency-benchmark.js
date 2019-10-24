'use strict';

const { LatencyBenchmark } = require('./lib/benchmarks');

LatencyBenchmark
  .create()
  .run();

