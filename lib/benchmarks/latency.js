'use strict';

const fs = require('fs').promises;
const path = require('path');

const Benchmark = require('./benchmark');

/**
 * A benchmark that measures response time behaviour.
 * Every few milliseconds, it sends a number of requests.
 */
class LatencyBenchmark extends Benchmark {
  static create() {
    const options = Benchmark.defineBasicOptions()
      .option('requests-per-second', {
        alias: 'r',
        describe: 'Determines the number of requests per second',
        number: true,
        default: 1000
      })
      .option('send-interval', {
        alias: 'i',
        describe: 'Determines the send interval in milliseconds',
        number: true,
        default: 20
      })
      .option('sample-length', {
        describe: 'Determines the amount of seconds for the latency sample',
        default: 30,
        number: true,
      })
      .parse();

    return new LatencyBenchmark(options);
  }

  constructor(options) {
    super(options);

    this.createDriverClient();
    this.createHistogram();
  }

  run() {
    this._start()
      .catch(err => {
        console.error(err);
        this.client.shutdown();
      });
  }

  async _start() {
    await this.client.connect();
    await this.workload.setup(this.client, this.options, true);

    console.log('Warming up');
    await this.workload.warmup();

    this._startScheduling();
  }

  _startScheduling() {
    let iterationCounter = 0;

    const sendInterval = this.options.sendInterval;
    // Total duration in seconds, convert to millis
    const sampleLength = this.options.sampleLength * 1000;
    const operationsPerIteration = Math.floor(this.options.requestsPerSecond * sendInterval / 1000);
    const iterations = Math.round(sampleLength / sendInterval);

    console.log(`Executing ${operationsPerIteration} operations every ${sendInterval}ms, a total of ${
      this.options.sampleLength} seconds`);

    const timer = setInterval(() => {
      if (iterationCounter++ === iterations) {
        this._finish(timer);
        return;
      }

      this._execute(operationsPerIteration)
        .catch(err => this._finish(timer, err));

    }, sendInterval);
  }

  _execute(length) {
    const promises = new Array(length);

    for (let i = 0; i < length; i++) {
      const startTime = process.hrtime();
      promises[i] = this.workload.next()
        .then(() => {
          const diff = process.hrtime(startTime);
          const latencyMicros = diff[0] * 1000000 + diff[1] / 1000;
          this.histogram.recordValue(latencyMicros);
        });
    }

    return Promise.all(promises);
  }

  async _finish(timer, err) {
    clearInterval(timer);

    try {
      if (err) {
        console.error(err);
      } else {
        await this._saveResults();
      }
      await this.client.shutdown();
    } catch (err) {
      console.error(err);
    }
  }

  async _saveResults() {
    console.log('Saving results');

    const resultsPath = await this.createResultsPath();
    await fs.writeFile(path.join(resultsPath, 'latency.txt'), this.histogram.outputPercentileDistribution());

    console.log(`Results saved to: ${resultsPath}`);
  }
}

module.exports = LatencyBenchmark;