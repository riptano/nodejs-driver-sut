'use strict';

const cluster = require('cluster');
const fs = require('fs').promises;
const path = require('path');
const cpus = require('os').cpus().length;

const Benchmark = require('./benchmark');

const nanosToMillis = 1000000;

// The first sample is going to be discarded
const maxSamples = 6;

class WorkersThroughputBenchmark {
  static createMaster() {
    const options = getOptions().parse();

    return new MasterBenchmark(options);
  }

  static createWorker() {
    const options = getOptions().parse();
    return new WorkerBenchmark(options);
  }
}

class MasterBenchmark extends Benchmark {
  constructor(options) {
    super(options);

    this._stopCounter = 0;
    this._warmupCounter = 0;
    this._resultArray = [];
    this._resultMap = new Map();
    this._concurrentGroupIndex = 0;
    this._groupSamples = 0;
    this._startTime = null;
    this._elapsed = null;
    this._workers = null;

    this._messageHandlers = new Map([
      [ 'setupFinished', this._setupHandler ],
      [ 'warmupFinished', this._warmupHandler ],
      [ 'startupComplete', () => {} ],
      [ 'stopCompleted', this._stopHandler ],
      [ 'countResult', this._countResult ],
      [ 'error', () => { throw new Error('An error was found in a worker'); } ],
      [ 'notify', msg => console.log(`Received on master`, msg.value)]
    ]);
  }

  run() {
    for (let i = 0; i < cpus; i++) {
      cluster.fork();
    }

    this._workers = Object.keys(cluster.workers).map(k => cluster.workers[k]);

    this._workers.forEach(worker => worker.on('message', msg => this._handleMessage(msg)));

    // Setup on a single worker
    console.log('Setting up');
    this._workers[0].send({ cmd: 'setup' });
  }

  _handleMessage(msg) {
    const handler = this._messageHandlers.get(msg.cmd);

    if (!handler) {
      throw new Error(`Unhandled command '${msg.cmd}'`);
    }

    return handler.call(this, msg);
  }

  _countResult(msg) {
    const count = msg.count;

    this._resultArray.push(count);

    if (this._resultArray.length === this._workers.length) {
      const totalCount = this._resultArray.reduce((acc, current) => acc + current);
      this._resultArray = [];


      if (this._elapsed !== null) {
        const elapsedMillis = this._elapsed[0] * 1000 + this._elapsed[1] / nanosToMillis;

        const throughput = totalCount * 1000 / elapsedMillis;

        const concurrentOps = this.options.concurrentOperations[this._concurrentGroupIndex];
        console.log(`Throughput: ${throughput.toFixed()} r/s`);

        let groupResultArray = this._resultMap.get(concurrentOps);
        if (!groupResultArray) {
          groupResultArray = [];
          this._resultMap.set(concurrentOps, groupResultArray);
        }

        groupResultArray.push(throughput);
      }
    }
  }

  _setupHandler() {
    // Warmup all workers
    console.log(`Warming up ${this._workers.length} workers`);
    this._workers.forEach(w => w.send({ cmd: 'warmup' }));
  }

  _stopHandler() {
    if (++this._stopCounter === this._workers.length) {
      // All workers stopped
      if (++this._concurrentGroupIndex < this.options.concurrentOperations.length) {
        this._stopCounter = 0;
        setTimeout(() => this._startAllWorkers(), 2000);
      } else {
        this._workers.forEach(w => w.send({ cmd: 'shutdown' }));

        setTimeout(() => {
          this._workers.forEach(w => w.disconnect());
          this._saveResults(this._resultMap);
        }, 1000);
      }
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

    console.log(`Results saved to: ${resultsPath}`);
  }

  _warmupHandler() {
    if (++this._warmupCounter === this._workers.length) {
      // All workers have warmed up
      this._startAllWorkers();
    }
  }

  _startAllWorkers() {
    this._groupSamples = 0;
    this._startTime = null;

    const totalConcurrentOps = this.options.concurrentOperations[this._concurrentGroupIndex];

    // Use rounding as there isn't much difference for +-1 concurrent op per worker
    const concurrentOps = Math.round(totalConcurrentOps / this._workers.length);

    if (concurrentOps <= 0) {
      throw new Error(
        `Concurrent operations are going to be partitioned across all workers and should be at least 1 per worker`);
    }

    console.log(`Executing iterations with ${concurrentOps} concurrent operations on each of the ${
      this._workers.length} workers (configured total ${totalConcurrentOps} concurrent ops)`);
    this._workers.forEach(w => w.send({ cmd: 'start', concurrentOps }));

    setTimeout(() => this._requestCount(), this.options.samplesInterval);
  }

  _requestCount() {
    if (this._startTime !== null) {
      this._elapsed = process.hrtime(this._startTime);
    } else {
      this._elapsed = null;
    }

    this._startTime = process.hrtime();

    if (this._groupSamples++ < maxSamples) {
      this._workers.forEach(w => w.send({ cmd: 'getCount' }));
      setTimeout(() => this._requestCount(), this.options.samplesInterval);
    } else {
      this._workers.forEach(w => w.send({ cmd: 'stop' }));
    }
  }
}

class WorkerBenchmark extends Benchmark {
  constructor(options) {

    options.skipLoggingWorkload = true;

    super(options);

    this.createDriverClient();

    this._messageHandlers = new Map([
      [ 'setup', this._setup ],
      [ 'warmup', this._warmup ],
      [ 'start', this._start ],
      [ 'shutdown', this._shutdown ],
      [ 'stop', this._stop ],
      [ 'getCount', this._getAndResetCount ]
    ]);

    this._concurrentOps = 0;
    this._isStopped = false;
    this._stopCounter = 0;
    this._opsCount = 0;
  }

  _handleMessage(msg) {
    const handler = this._messageHandlers.get(msg.cmd);

    if (!handler) {
      throw new Error(`Unhandled worker command '${msg.cmd}'`);
    }

    Promise
      .resolve(handler.call(this, msg))
      .catch(err => {
        console.error(err);
        return process.send({cmd: 'error', err });
      });
  }

  run() {
    // The worker will wait for master commands
    process.on('message', msg => this._handleMessage(msg));
  }

  async _setup() {
    await this.client.connect();

    await this.workload.setup(this.client, this.options, true);

    process.send({ cmd: 'setupFinished' });
  }

  async _warmup() {
    await this.client.connect();

    // Set up without recreating the schema
    await this.workload.setup(this.client, this.options, false);

    await this.workload.warmup();

    process.send({ cmd: 'warmupFinished' });
  }

  async _start(msg) {
    this._isStopped = false;
    this._concurrentOps = msg.concurrentOps;

    const promises = new Array(this._concurrentOps);
    for (let i = 0; i < this._concurrentOps; i++) {
      promises[i] = this.executeOneAtATime(i);
    }

    process.send({ cmd: 'startupComplete' });

    await Promise.all(promises);
  }

  async executeOneAtATime() {
    while (!this._isStopped) {
      await this.workload.next();
      this._opsCount++;
    }

    this.markStopExecution();
  }

  _getAndResetCount() {
    const count = this._opsCount;
    this._opsCount = 0;

    process.send({ cmd: 'countResult', count });
  }

  _stop() {
    this._isStopped = true;
  }

  markStopExecution() {
    if (++this._stopCounter === this._concurrentOps) {
      this._stopCounter = 0;
      this._opsCount = 0;
      process.send({ cmd: 'stopCompleted' });
    }
  }

  async _shutdown() {
    await this.client.shutdown();
  }
}

function getOptions() {
  return Benchmark.defineBasicOptions()
    .option('concurrent-operations', {
      alias: 'o',
      describe: 'Determines the maximum number of concurrent operations per iteration group',
      array: true,
      default: [128, 256, 512, 1024]
    })
    .option('requests-per-iteration', {
      alias: 'r',
      describe: 'Determines the number of request made per iteration per worker',
      number: true,
      default: 250000
    })
    .option('samples-interval', {
      describe: 'Determines the amount of milliseconds for each sample',
      default: 5000,
      number: true,
    });
}

module.exports = WorkersThroughputBenchmark;