'use strict';

class Workload {
  constructor() {
    this.description = '{no description}';
  }

  /**
   * Performs initialization needed to run the benchmark.
   * @returns {Promise}
   */
  setup(client, options, createSchema) {
    this.client = client;
    return Promise.resolve();
  }

  /**
   * Performs basic warmup
   * @returns {Promise}
   */
  warmup() {

  }

  async executeTimes(length, limit, fn) {
    const context = {
      counter: 0,
      length,
      fn
    };

    const promises = new Array(limit);
    for (let i = 0; i < limit; i++) {
      promises[i] = this._executeOneAtATime(context);
    }

    await Promise.all(promises);
  }

  async _executeOneAtATime(context) {
    let index;
    while ((index = context.counter++) < context.length) {
      await context.fn(index);
    }
  }

  /**
   * Executes the next task to benchmark.
   * @returns {Promise}
   */
  next() {
    throw new Error('Not implemented');
  }
}

/**
 * Use "Standard" workload with read and write operations
 * As defined in cassandra-stress:
 * https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/tools/toolsCStress.html#toolsCStress__cassandra-stress-from-yaml#toolsCStress__standare-workload-keyspace
 */
class StandardWorkload extends Workload {
  constructor() {
    super();

    this.description = 'Standard workload with read and write operations';

    this.insertQuery = 'INSERT INTO standard1 (key, c0, c1, c2, c3, c4) VALUES (?, ?, ?, ?, ?, ?)';
    this.selectQuery = 'SELECT key, c0, c1, c2, c3, c4 FROM standard1 WHERE key = ?';
    this.options = { prepare: true };

    this.warmupLength = 1000;
    this.index = 0;
  }

  async setup(client, options, createSchema) {
    super.setup(client, options, createSchema);

    const keyspace = 'ks_benchmarks_standard';

    if (!createSchema) {
      await client.execute(`USE ${keyspace}`);
      return;
    }

    const replicationFactor = client.hosts.length >= 3 ? 3 : 1;
    const firstHost = client.hosts.values()[0];

    const createKsQuery = `CREATE KEYSPACE ${keyspace}
      WITH replication = {'class': 'NetworkTopologyStrategy', '${firstHost.datacenter}' : ${replicationFactor} }
      AND durable_writes = false;`;

    const createTableQuery = `CREATE TABLE standard1
      (key blob, c0 blob, c1 blob, c2 blob, c3 blob, c4 blob, PRIMARY KEY (key))`;

    await client.execute(`DROP KEYSPACE IF EXISTS ${keyspace}`);
    await client.execute(createKsQuery);
    await client.execute(`USE ${keyspace}`);
    await client.execute(createTableQuery);
  }

  _getIntBuffer(i) {
    const b = Buffer.allocUnsafe(4);
    b.writeInt32BE(i, 0);
    return b;
  }

  async warmup() {
    await super.warmup();

    const concurrencyLevel = 32;

    // Execute some INSERT queries
    await this.executeTimes(this.warmupLength, concurrencyLevel, i => {
      const b = this._getIntBuffer(i);
      return this.client.execute(this.insertQuery, [b, b, b, b, b, b], this.options);
    });

    // Execute some SELECT queries
    await this.executeTimes(this.warmupLength, concurrencyLevel, i =>
      this.client.execute(this.selectQuery, [this._getIntBuffer(i % this.warmupLength)], this.options));
  }

  next() {
    const index = this.index++;

    if (index % 2 === 0) {
      return this.nextSelect(index);
    }

    return this.nextInsert(index);
  }

  nextSelect(index) {
    return this.client.execute(this.selectQuery, [ this._getIntBuffer(index % this.warmupLength) ], this.options);
  }

  nextInsert(index) {
    const b = this._getIntBuffer(index);
    return this.client.execute(this.insertQuery, [ b, b, b, b, b, b ], this.options);
  }
}

class StandardReadWorkload extends StandardWorkload {
  constructor() {
    super();

    this.description = 'Standard workload with read operations only';
  }

  next() {
    return this.nextSelect(this.index++);
  }
}

class StandardWriteWorkload extends StandardWorkload {
  constructor() {
    super();

    this.description = 'Standard workload with write operations only';
  }

  next() {
    return this.nextInsert(this.index++);
  }
}

/**
 * Uses system.local table
 */
class BasicWorkload extends Workload {
  constructor() {
    super();

    this.description = 'system.local table';

    this.options = { prepare: true };
    this.params = [];
  }

  next() {
    return this.client.execute('SELECT key FROM system.local', this.params, this.options);
  }
}

/**
 * Use a table with a single blob
 */
class MinimalWorkload extends Workload {
  constructor() {
    super();

    this.description = 'a minimal workload that inserts a single byte';

    this.query = 'INSERT INTO table1 (key) VALUES (?)';
    this.params = [ Buffer.alloc(1, 1) ];
    this.options = { prepare: true };
  }

  async setup(client, options, createSchema) {
    super.setup(client, options, createSchema);

    const keyspace = 'ks_benchmarks_minimal';

    if (!createSchema) {
      await client.execute(`USE ${keyspace}`);
      return;
    }

    const replicationFactor = client.hosts.length >= 3 ? 3 : 1;
    const firstHost = client.hosts.values()[0];

    const createKsQuery = `CREATE KEYSPACE ${keyspace}
      WITH replication = {'class': 'NetworkTopologyStrategy', '${firstHost.datacenter}' : ${replicationFactor} }
      AND durable_writes = false;`;

    const createTableQuery = `CREATE TABLE table1 (key blob PRIMARY KEY)`;

    await client.execute(`DROP KEYSPACE IF EXISTS ${keyspace}`);
    await client.execute(createKsQuery);
    await client.execute(`USE ${keyspace}`);
    await client.execute(createTableQuery);
  }

  async warmup() {
    await super.warmup();

    // Execute some INSERT queries
    await this.executeTimes(1000, 32, () => this.client.execute(this.query, this.params, this.options));
  }

  next() {
    return this.client.execute(this.query, this.params, this.options);
  }
}

const workloads = [
  StandardWorkload, BasicWorkload, StandardReadWorkload, StandardWriteWorkload, MinimalWorkload
];
const names = new Map(workloads.map(c => [ transformName(c.name), c ]));

function getByName (name) {
  const w = names.get(name);

  if (!w) {
    throw new Error(`Workload '${name}' not found`);
  }

  return w;
}

function getNames() {
  return Array.from(names.keys());
}

function transformName(name) {
  return name.replace('Workload', '').toLowerCase();
}

module.exports = {
  getByName,
  getNames
};