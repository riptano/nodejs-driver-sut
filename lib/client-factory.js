'use strict';

const clientConfig = require('../config/client-config');

class ClientFactory {
  static create(options) {
    // eslint-disable-next-line global-require
    const driver = require(options.driver === 'core' ? 'cassandra-driver' : 'dse-driver');

    return new driver.Client(Object.assign({
      contactPoints: options.contactPoints,
      localDataCenter: options.dc
    }, ClientFactory._parseOptionsTokens(options, driver), clientConfig(driver)));
  }

  static _parseOptionsTokens(options, driver) {
    const parser = new OptionsTokenParser(driver);
    return parser.parse(options.clientOptions);
  }
}

class OptionsTokenParser {
  constructor(driver) {
    this._driver = driver;
    this._result = { policies: {}, queryOptions: {} };
    this._tokens = new Map([
      ['lbp', this._parseLbp],
      ['spec-exec', this._parseSpecExec],
      ['consistency', this._parseConsistency]
    ]);
  }

  parse(optionsArray) {
    if (optionsArray && optionsArray.length > 0) {
      for (const token of optionsArray) {
        if (!token) {
          continue;
        }

        const tokenParts = token.split(':');
        const tokenKey = tokenParts[0];
        const handler = this._tokens.get(tokenKey);

        if (!handler) {
          throw new Error(`Parser for token '${tokenKey}' not found`);
        }

        handler.call(this, tokenParts[1]);
      }
    }

    return this._result;
  }

  _parseLbp(name) {
    const root = this._driver.policies.loadBalancing;
    let lbp;
    switch (name) {
      case 'tap':
        lbp = new root.TokenAwarePolicy(new root.DCAwareRoundRobinPolicy());
        break;
      case 'rr':
        lbp = new root.RoundRobinPolicy();
        break;
      default:
        throw new Error(`LBP with name ${name} not found`);
    }

    this._result.policies.loadBalancing = lbp;
  }

  _parseSpecExec(name) {
    const root = this._driver.policies.speculativeExecution;
    const values = name.split('|');
    let policy;
    switch (values[0]) {
      case 'constant':
        policy = new root.ConstantSpeculativeExecutionPolicy(+values[1] || 100, +values[2] || 2);
        break;
      case 'no':
        policy = new root.NoSpeculativeExecutionPolicy();
        break;
      default:
        throw new Error(`Policy with name ${name} not found`);
    }

    this._result.policies.speculativeExecution = policy;
  }

  _parseConsistency(name) {
    const types = this._driver.types;
    let consistency;
    switch (name) {
      case 'lq':
        consistency = types.consistencies.localQuorum;
        break;
      case 'lo':
        consistency = types.consistencies.localOne;
        break;
      case 'all':
        consistency = types.consistencies.all;
        break;
      default:
        throw new Error(`Consistency level with name ${name} not found`);
    }

    this._result.queryOptions.consistency = consistency;
  }
}
module.exports = ClientFactory;