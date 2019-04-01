'use strict';

const cluster = require('cluster');
const assert = require('assert');
const cpus = require('os').cpus().length;
const utils = require('../src/utils');
const cmdLineOptions = utils.parseCommonOptions();
const driver = require(cmdLineOptions.driverPackageName);

class Master {
  constructor() {
    this.connectCounter = 0;
    this.warmupCounter = 0;
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
      case 'iterationFinished':
        console.log('iterationFinished', msg);
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
    }
  }
}

class Worker {
  constructor() {
    this.selectQuery = 'SELECT key FROM system.local';
    this.opsCount = 0;
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
        case 'getCount':
          this.getAndResetCount();
          break;
      }
    });

    // const options = utils.connectOptions();
    const options = { contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1' };
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
    const nanosInASecond = BigInt('1000000000');
    const count = 1000;

    utils.series(
      [
        next => {
          utils.timesSeries(10, (n, timesSeriesNext) => {

            const started = process.hrtime.bigint();

            utils.timesLimit(
              count, 64,
              (n, timesNext) => this.client.execute(this.selectQuery, timesNext),
              err => {
                if (!err) {
                  const diffNanos = process.hrtime.bigint() - started;
                  process.send({
                    cmd: 'iterationFinished',
                    throughput:  (BigInt(count) * nanosInASecond * diffNanos).toString()
                  });
                }
                timesSeriesNext(err);
              });
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
        process.send({ cmd: 'execute finished' });
        this.client.shutdown();
      });
  }

  getAndResetCount() {
    const count = this.opsCount;
    this.opsCount = 0;

  }
}

if (cluster.isMaster) {
  const master = new Master();
  master.init();
} else {
  const worker = new Worker();
  worker.init();
}