'use strict';

const cluster = require('cluster');
const assert = require('assert');
const cpus = require('os').cpus().length;
const utils = require('../src/utils');
const cmdLineOptions = utils.parseCommonOptions();
const driver = require(cmdLineOptions.driverPackageName);

const concurrencyLevel = 300;
const delay = 1000;
const maxSamples = 10;
const nanosToMillis = 1000000;

class Master {
  constructor() {
    this.connectCounter = 0;
    this.stopCounter = 0;
    this.warmupCounter = 0;
    this.resultArray = [];
    this.totalSamples = 0;
    this.startTime = null;
    this.elapsed = null;
  }

  init() {

    // Fork workers.
    for (let i = 0; i < cpus; i++) {
      cluster.fork();
    }

    this.workers = Object.keys(cluster.workers).map(k => cluster.workers[k]);

    cluster.on('exit', (worker, code) => {
      //console.log(`worker ${worker.process.pid} exited with code ${code}`);
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
      case 'stopCompleted':
        this.stopHandler();
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
      this.workers.forEach(w => w.send({ cmd: 'warmup' }));
    }
  }

  stopHandler() {
    if (++this.stopCounter === this.workers.length) {
      // All workers are connected
      this.workers.forEach(w => w.send({ cmd: 'shutdown' }));
    }
  }

  warmupHandler() {
    if (++this.warmupCounter === this.workers.length) {
      // All workers are connected
      this.workers.forEach(w => w.send({ cmd: 'start' }));
      setTimeout(() => this.requestCount(), 200);
    }
  }

  countResult(count) {
    this.resultArray.push(count);

    if (this.resultArray.length === this.workers.length) {
      const totalCount = this.resultArray.reduce((acc, current) => acc + current);
      this.resultArray = [];

      if (this.elapsed !== null) {
        const elapsedMillis = this.elapsed[0] * 1000 + Math.floor(this.elapsed[1] / nanosToMillis);

        const throughput = Math.floor(totalCount * 1000 / elapsedMillis);

        console.log(`${concurrencyLevel} ${throughput}`);
      }
    }
  }

  requestCount() {
    this.workers.forEach(w => w.send({ cmd: 'getCount' }));

    if (this.startTime !== null) {
      this.elapsed = process.hrtime(this.startTime);
    }

    this.startTime = process.hrtime();

    if (this.totalSamples++ < maxSamples) {
      setTimeout(() => this.requestCount(), delay);
    } else {
      this.workers.forEach(w => w.send({ cmd: 'stop' }));
      setTimeout(() => this.workers.forEach(w => w.disconnect()), 1000);
    }
  }
}

class Worker {
  constructor() {
    this.opsCount = 0;
    this.isStopped = false;
    this.stopCounter = 0;
    this.queryOptions = { prepare: true, isIdempotent: true };
  }

  init() {
    process.on('message', msg => {
      switch (msg.cmd) {
        case 'warmup':
          this.warmup();
          break;
        case 'start':
          this.start();
          break;
        case 'shutdown':
          this.shutdown();
          break;
        case 'stop':
          this.setStopWorkload();
          break;
        case 'getCount':
          this.getAndResetCount();
          break;
      }
    });

    //const options = utils.connectOptions();
    const options = { contactPoints: ['127.0.0.2'], localDataCenter: 'dc1' };
    this.client = new driver.Client(options);
    this.client.connect(err => {
      assert.ifError(err);
      process.send({ cmd: 'connected', value: Date.now() });
    });
  }

  warmup() {
    utils.series(
      [
        next => {
          utils.timesLimit(1000, 10, (n, timesNext) => {
            this.client.execute(this.getQuery(), this.getParameters(), timesNext);
          }, next);
        }
      ],
      err => {
        assert.ifError(err);
        process.send({ cmd: 'warmupFinished' });
      });
  }

  start() {

    for (let i = 0; i < concurrencyLevel; i++) {
      this.executeOneAtATime(i);
    }

    process.send({ cmd: 'startupComplete' });
  }

  executeOneAtATime(index) {
    if (this.isStopped) {
      return this.stopWorkload();
    }

    this.client.execute(this.getQuery(index), this.getParameters(index), this.queryOptions, err => {
      assert.ifError(err);

      this.opsCount++;
      this.executeOneAtATime(index);
    });
  }

  getQuery(index) {
    return 'SELECT key FROM system.local';
  }

  getParameters(index) {
    return null;
  }

  getAndResetCount() {
    const count = this.opsCount;
    this.opsCount = 0;

    process.send({ cmd: 'countResult', count });
  }

  setStopWorkload() {
    this.isStopped = true;
  }

  stopWorkload() {
    if (++this.stopCounter === concurrencyLevel) {
      process.send({ cmd: 'stopCompleted' });
      this.stopCounter = 0;
    }
  }

  shutdown() {
    this.client.shutdown();
  }
}

if (cluster.isMaster) {
  const master = new Master();
  master.init();
} else {
  const worker = new Worker();
  worker.init();
}