'use strict';

const cluster = require('cluster');
const assert = require('assert');
const cpus = require('os').cpus().length;
const utils = require('../src/utils');
const cmdLineOptions = utils.parseCommonOptions();
const driver = require(cmdLineOptions.driverPackageName);

const concurrencyLevel = 128;
const delay = 1000;
const maxSamples = 25;
const nanosToMillis = BigInt('1000000');

class Master {
  constructor() {
    this.connectCounter = 0;
    this.warmupCounter = 0;
    this.resultArray = [];
    this.totalSamples = 0;
    this.startTime = null;
    this.elapsed = null;
  }

  init() {
    console.log(`Master ${process.pid} is running`);

    // Fork workers.
    for (let i = 0; i < cpus; i++) {
      cluster.fork();
    }

    this.workers = Object.keys(cluster.workers).map(k => cluster.workers[k]);

    cluster.on('exit', (worker, code) => {
      console.log(`worker ${worker.process.pid} exited with code ${code}`);
    });

    this.workers.forEach(worker => worker.on('message', msg => this.messageHandler(msg)));
  }

  messageHandler(msg) {
    switch (msg.cmd) {
      case 'connected':
        this.connectHandler(msg);
        break;
      case 'warmupFinished':
        this.warmupHandler();
        break;
      case 'countResult':
        this.countResult(msg.count);
        break;
      case 'notify':
        console.log(`Received on master`, msg.value);
        break;
    }
  }

  connectHandler() {
    if (++this.connectCounter === this.workers.length) {
      // All workers are connected
      console.log('All workers connected!');
      this.workers.forEach(w => w.send({ cmd: 'warmup' }));
    }
  }

  warmupHandler() {
    if (++this.warmupCounter === this.workers.length) {
      // All workers are connected
      console.log('All workers warmed up!');
      this.workers.forEach(w => w.send({ cmd: 'start' }));
      setTimeout(() => this.requestCount(), 200);
    }
  }

  countResult(count) {
    this.resultArray.push(count);

    if (this.resultArray.length === this.workers.length) {
      console.log('All workers returned count!');
      const totalCount = this.resultArray.reduce((acc, current) => acc + current);
      this.resultArray = [];

      if (this.elapsed !== null) {
        // x     ____ 1000 millis
        // total ____ elapsedMillis (elapsed/nanosInMillis)
        const throughput = Number(BigInt(totalCount) * BigInt(1000) * nanosToMillis / this.elapsed);

        console.log(`Throughput ${throughput} ops/s (${this.elapsed/nanosToMillis} ms)`);
      }
    }
  }

  requestCount() {
    this.workers.forEach(w => w.send({ cmd: 'getCount' }));

    if (this.startTime !== null) {
      this.elapsed = process.hrtime.bigint() - this.startTime;
    }

    this.startTime = process.hrtime.bigint();

    if (this.totalSamples++ < maxSamples) {
      setTimeout(() => this.requestCount(), delay);
    } else {
      this.workers.forEach(w => w.send({ cmd: 'finish' }));
      setTimeout(() => this.workers.forEach(w => w.disconnect()), 1000);
    }
  }
}

class Worker {
  constructor() {
    this.selectQuery = 'SELECT key FROM system.local';
    this.opsCount = 0;
    this.finished = false;
    this.queryOptions = { prepare: true, isIdempotent: true };
  }

  init() {
    process.on('message', msg => {
      switch (msg.cmd) {
        case 'warmup':
          console.log('warmup received on worker');
          this.warmup();
          break;
        case 'start':
          this.start();
          break;
        case 'finish':
          this.finish();
          break;
        case 'getCount':
          this.getAndResetCount();
          break;
      }
    });

    // const options = utils.connectOptions();
    const options = { contactPoints: ['127.0.0.2'], localDataCenter: 'dc1' };
    this.client = new driver.Client(options);
    this.client.connect(err => {
      assert.ifError(err);
      process.send({ cmd: 'connected', value: Date.now() });
    });

    console.log(`Worker ${process.pid} started`);
  }

  warmup() {
    console.log('warmup starting');
    utils.series(
      [
        next => {
          utils.timesLimit(10, 10, (n, timesNext) => {
            this.client.execute(this.selectQuery, timesNext);
          }, next);
        },
        next => {
          next();
        },
        next => {
          next();
        }
      ],
      err => {
        assert.ifError(err);
        process.send({ cmd: 'warmupFinished' });
      });
  }

  start() {

    for (let i = 0; i < concurrencyLevel; i++) {
      this.executeOneAtATime();
    }

    process.send({ cmd: 'startupComplete' });
  }

  executeOneAtATime() {
    if (this.finished) {
      this.client.shutdown();
      return;
    }

    this.client.execute(this.selectQuery, null, this.queryOptions, err => {
      if (!this.finished) {
        assert.ifError(err);
      }

      this.opsCount++;
      this.executeOneAtATime();
    });
  }

  getAndResetCount() {
    const count = this.opsCount;
    this.opsCount = 0;

    process.send({ cmd: 'countResult', count });
  }

  finish() {
    this.finished = true;
  }
}

if (cluster.isMaster) {
  const master = new Master();
  master.init();
} else {
  const worker = new Worker();
  worker.init();
}