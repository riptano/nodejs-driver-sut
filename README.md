# DataStax Node.js Driver SUT

HTTP wrapper / shell-based client for the DataStax Node.js driver for Apache Cassandra, suitable for benchmarking.

## Installation

```bash
npm install
# install the driver using a specific branch or tag
npm install datastax/nodejs-driver#master
```

## Usage samples

### Using shell

The shell tests execute insert and select queries multiple times and display throughput and latency metrics.

**Example:**

Execute 1,000,000 requests with 512 as maximum outstanding requests (in-flight), using 192.168.1.100 as Cassandra
cluster contact point.

```bash
node test/throughput-test.js -c 192.168.1.100 -r 1000000 -o 512
```

You can also use `-h` or `--help` to display the help text with available options.

```bash
node test/throughput-test.js --help
```

### Using web interface

The web interface executes to insert and select statements.


```bash
node src/server.js
```

