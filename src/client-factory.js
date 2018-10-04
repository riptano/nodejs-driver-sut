'use strict';

const dse = require('dse-driver');

/**
 * Module designed to separate Client instance creation (with different options) from the benchmark.
 */

module.exports = {
  createAndConnect: async function () {
    const client = new dse.Client({
      contactPoints: [ process.argv[2] || '127.0.0.1']
    });

    await client.connect();
    return client;
  }
};