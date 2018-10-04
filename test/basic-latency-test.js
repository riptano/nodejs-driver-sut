'use strict';

const fs = require('fs');
const hdr = require('hdr-histogram-js');
const dse = require('dse-driver');

const iterations = 200;
const timesPerIteration = 100;
const delay = 20;

const histogram = hdr.build({
  bitBucketSize: 32,
  highestTrackableValue: 20000,
  numberOfSignificantValueDigits: 5
});

async function connect() {
  const contactPoints = [ process.argv[2] || '127.0.0.1' ];
  const client = new dse.Client({ contactPoints, protocolOptions: { maxVersion: 4 } });

  await client.connect();
  return client;
}

async function startBenchmark(client) {
  console.log(`Getting started using hosts: ${client.hosts.values().map(h => h.address)}`);

  // warmup
  await Promise.all(new Array(128).fill(0).map(() => {
    client.execute('SELECT * FROM system.local');
  }));

  let firstError;
  let iterationCounter = 0;

  const intervalTimer = setInterval(() => {
    if (iterationCounter++ === iterations) {
      clearInterval(intervalTimer);
      finish(client);
      return;
    }

    for (let i = 0; i < timesPerIteration; i++) {
      const startTime = process.hrtime();
      client.execute('SELECT * FROM system.local', (err) => {
        if (firstError) {
          return;
        }

        if (err) {
          firstError = err;
          clearInterval(intervalTimer);
          console.error(err);
        }

        const diff = process.hrtime(startTime);
        const latencyMs = diff[0] * 1000000 + diff[1] / 1000;
        histogram.recordValue(latencyMs);
      });
    }
  }, delay);
}

function finish(client) {
  console.log('Finished');

  const output = histogram.outputPercentileDistribution();
  fs.writeFileSync('latency.txt', output);

  setTimeout(() => client.shutdown(), 1000);
}

connect()
  .then(client => startBenchmark(client));