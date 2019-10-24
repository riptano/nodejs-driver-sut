'use strict';

const fs = require('fs').promises;
const path = require('path');

const Benchmark = require('./benchmark');

const iterations = 5;

class ThroughputBenchmark extends Benchmark {
  static create() {
    const options = Benchmark.defineBasicOptions()
      .option('concurrent-operations', {
        alias: 'o',
        describe: 'Determines the maximum number of concurrent operations per iteration group',
        array: true,
        default: [ 128, 256, 512, 1024 ]
      })
      .option('requests-per-iteration', {
        alias: 'r',
        describe: 'Determines the number of request made per iteration',
        number: true,
        default: 250000
      })
      .option('track-latency', {
        describe: 'Determines whether the benchmark should record latency',
        default: true,
        boolean: true,
        choices: [true, false]
      })
      .parse();

    return new ThroughputBenchmark(options);
  }

  constructor(options) {
    super(options);

    this.createDriverClient();

    if (options.trackLatency) {
      this.createHistogram();
    }
  }

  async _start() {
    await this.client.connect();
    await this.workload.setup(this.client, this.options, true);

    console.log('Warming up');
    await this.workload.warmup();

    const results = new Map();
    for (let j = 0; j < this.options.concurrentOperations.length; j++) {
      const concurrentOps = this.options.concurrentOperations[j];
      const groupResults = [];
      results.set(concurrentOps, groupResults);

      console.log(`Executing iterations of ${this.options.requestsPerIteration} operations w/ ${
        concurrentOps} concurrent ops`);

      for (let i = 0; i < iterations; i++) {
        groupResults.push(await this._performIteration(concurrentOps));
      }
    }

    await this._saveResults(results);

    await this.client.shutdown();
  }

  async _performIteration(concurrentOps) {
    const promises = new Array(concurrentOps);

    const context = {
      counter: 0,
      totalLength: this.options.requestsPerIteration,
      trackStart: () => {},
      trackEnd: () => {}
    };

    if (this.options.trackLatency) {
      context.trackStart = () => process.hrtime();
      context.trackEnd = start => {
        const diff = process.hrtime(start);
        // Record value in micros
        this.histogram.recordValue(diff[0] * 1000000 + diff[1] / 1000);
      };
    }

    for (let i = 0; i < concurrentOps; i++) {
      promises[i] = this._executeOneAtATime(context);
    }

    const start = process.hrtime();
    await Promise.all(promises);
    const diff = process.hrtime(start);
    const diffMs = diff[0] * 1000 + diff[1] / 1000000;
    const throughput = this.options.requestsPerIteration * 1000 / diffMs;

    console.log(`Throughput: ${throughput.toFixed()} r/s`);

    return throughput;
  }

  async _executeOneAtATime(context) {
    while (context.counter++ < context.totalLength) {
      // await this.workload.next();
      const p = this.workload.next();
      const start = context.trackStart();
      await p;
      context.trackEnd(start);
    }
  }

  async _saveResults(results) {
    let data = '';

    results.forEach((arr, key) => {
      data += `${key} \n`;
      arr.forEach(v => {
        data += `${key} ${v}\n`;
      });
      data += `${key} thrpt\n`;
    });


    console.log('Saving results');

    const resultsPath = await this.createResultsPath();
    await fs.writeFile(path.join(resultsPath, 'throughput.txt'), data);

    if (this.options.trackLatency) {
      await fs.writeFile(path.join(resultsPath, 'latency.txt'), this.histogram.outputPercentileDistribution());
    }

    console.log(`Results saved to: ${resultsPath}`);
  }

  run() {
    this._start()
      .catch(console.error);
  }
}

module.exports = ThroughputBenchmark;