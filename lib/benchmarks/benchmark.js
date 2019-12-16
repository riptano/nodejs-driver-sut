'use strict';

const yargs = require('yargs');
const mkdirp = require('mkdirp');
const hdr = require('hdr-histogram-js');
const path = require('path');

const workloads = require('../workloads');
const clientConfig = require('../../config/client-config');

class Benchmark {
  constructor(options) {
    this.options = options;

    const workloadConstructor = workloads.getByName(options.workload);

    this.workload = new (workloadConstructor)();

    if (!options.skipLoggingWorkload) {
      console.log(`Using ${this.workload.description}`);
    }

    this.client = null;
    this.histogram = null;
  }

  createDriverClient() {
    // eslint-disable-next-line global-require
    const driver = require(this.options.driver === 'core' ? 'cassandra-driver' : 'dse-driver');

    this.client = new driver.Client(Object.assign({
      contactPoints: this.options.contactPoints,
      localDataCenter: this.options.dc
    }, clientConfig(driver)));
  }

  createHistogram() {
    this.histogram = hdr.build({
      highestTrackableValue: 400000,
      numberOfSignificantValueDigits: 4
    });
  }

  static defineBasicOptions() {
    return yargs
      .option('workload', {
        alias: 'w',
        describe: 'Choose a workload',
        default: 'standard',
        choices: workloads.getNames()
      })
      .option('contact-points', {
        alias: 'c',
        describe: 'Choose one or more contact points',
        required: true,
        array: true
      })
      .option('dc', {
        describe: 'Choose a local data center',
        default: 'dc1'
      })
      .option('folder', {
        alias: 'f',
        describe: 'Determines the name of the directory to be used for the results',
        required: true
      })
      .option('driver', {
        describe: 'Choose a driver type',
        default: 'core',
        choices: ['core', 'dse']
      });
  }

  async createResultsPath() {
    const resultsPath = path.join('results', this.options.folder);
    return await new Promise((resolve, reject) => mkdirp(resultsPath, err => {
      if (err) {
        reject(err);
      } else {
        resolve(resultsPath);
      }
    }));
  }
}

module.exports = Benchmark;