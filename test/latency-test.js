'use strict';

const fs = require('fs');
const hdr = require('hdr-histogram-js');
const dse = require('dse-driver');
const utils = require('../src/utils');

const iterations = 1000;
const timesPerIteration = 50;
const delay = 20;

const histogram = hdr.build({
  bitBucketSize: 32,
  highestTrackableValue: 400000,
  numberOfSignificantValueDigits: 4
});

async function startBenchmark(client, workload) {
  console.log(`Starting using hosts: ${client.hosts.values().map(h => h.address)}`);

  await workload.init();

  await workload.warmup();

  console.log(client.getState().toString());

  console.log('Warmup finished, start scheduling');

  let firstError;
  let iterationCounter = 0;

  const intervalTimer = setInterval(() => {
    if (iterationCounter++ === iterations) {
      clearInterval(intervalTimer);
      finish(client);
      return;
    }

    for (let i = 0; i < timesPerIteration; i++) {
      workload.execute(err => {
        if (err && !firstError) {
          firstError = err;
          clearInterval(intervalTimer);
          console.error(err);
        }
      });
    }
  }, delay);
}

function finish(client) {
  console.log('Execution finished, saving output file');

  const output = histogram.outputPercentileDistribution();
  fs.writeFileSync('latency.txt', output);

  setTimeout(() => client.shutdown(), 1000);
}

const client = new dse.Client(utils.connectOptions());

client.on('log', (level, className, message) => {
  if (level !== 'verbose') {
    console.log(level, className, message);
  }
});

client
  .connect()
  .then(() => {
    client.on('hostDown', host => console.log(`-----Host ${host.address} is considered DOWN-----`));
    client.on('hostUp', host => console.log(`-----Host ${host.address} is considered back UP-----`));

    setInterval(() => console.log(client.getState().toString()), 10000);

    return startBenchmark(client, new MixedWorkload(client, histogram, 1));
  });


class BasicWorkload {
  constructor(client, histogram) {
    this.client = client;
    this.histogram = histogram;
  }

  async createSchema() {

  }

  async warmup() {
    await Promise.all(new Array(256).fill(0).map(() => {
      this.client.execute('SELECT * FROM system.local');
    }))
  }

  execute(callback) {
    const startTime = process.hrtime();
    this.client.execute('SELECT * FROM system.local', (err) => {
      const diff = process.hrtime(startTime);
      const latencyMs = diff[0] * 1000000 + diff[1] / 1000;
      this.histogram.recordValue(latencyMs);
      callback(err);
    });
  }
}

class MixedWorkload {
  constructor(client, histogram, replicationFactor) {
    this.client = client;
    this.histogram = histogram;
    this.replicationFactor = replicationFactor;
    this.insertQuery = 'INSERT INTO standard1 (key, c0, c1) VALUES (?, ?, ?)';
    this.selectQuery = 'SELECT c0 FROM standard1 WHERE key = ?';
    this.executeIndex = 0;

    this.queryOptions = {
      prepare: true,
      isIdempotent: true,
      consistency: replicationFactor >= 3
        ? dse.types.consistencies.localQuorum
        : dse.types.consistencies.localOne
    };
  }

  async init() {
    const queries = [
      // `USE ks_benchmarks_rf${this.replicationFactor}`,
      // 'DROP TABLE IF EXISTS standard1',
      'CREATE TABLE IF NOT EXISTS standard1 (key blob PRIMARY KEY,c0 blob,c1 blob,c2 blob,c3 blob,c4 blob)'
    ];

    for (const q of queries) {
      await this.client.execute(q);
    }
  }

  async warmup() {
    const b = Buffer.alloc(4);

    await Promise.all(new Array(256).fill(0).map(() =>
      this.client.execute(this.insertQuery, [b, b, b], this.queryOptions)));

    await Promise.all(new Array(256).fill(0).map(() =>
      this.client.execute(this.selectQuery, [b], this.queryOptions)));
  }

  execute(callback) {
    const index = this.executeIndex++;
    const b = Buffer.allocUnsafe(4);
    const startTime = process.hrtime();

    let query;
    let params;

    if (index % 2 === 0) {
      query = this.selectQuery;
      b.writeInt32BE(index / 2, 0);
      params = [ b ]
    } else {
      query = this.insertQuery;
      b.writeInt32BE(index, 0);
      params = [ b, b, b ]
    }

    this.client.execute(query, params, this.queryOptions, (err) => {
      const diff = process.hrtime(startTime);
      const latencyMs = diff[0] * 1e9 + diff[1] / 1e3;
      this.histogram.recordValue(latencyMs);
      callback(err);
    });
  }
}