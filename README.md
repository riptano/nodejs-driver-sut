# DataStax Node.js Driver SUT

Command line tools for the [DataStax Node.js Driver][driver], suitable for benchmarking.

## Installation

```bash
npm install
```

Install the driver using a specific branch or tag

```bash
npm install datastax/nodejs-driver#master
```

## Usage

There are interactive command line Node.js scripts included in the root of the repository.

Using `--help`, you can get the options to run each benchmark. For example:

```bash
nodejs-driver-sut user$ node throughput-benchmark.js --help
```

```
Options:
  --help                        Show help                              [boolean]
  --version                     Show version number                    [boolean]
  --workload, -w                Choose a workload
                            [choices: "standard", "basic"] [default: "standard"]
  --contact-points, -c          Choose one or more contact points
                                                              [array] [required]
  --dc                          Choose a local data center      [default: "dc1"]
  --folder, -f                  Determines the name of the directory to be used
                                for the results                       [required]
  --driver                      Choose a driver type
                                      [choices: "core", "dse"] [default: "core"]
  --concurrent-operations, -o   Determines the maximum number of concurrent
                                operations per iteration group
                                           [array] [default: [128,256,512,1024]]
  --requests-per-iteration, -r  Determines the number of request made per
                                iteration             [number] [default: 250000]
  --track-latency               Determines whether the benchmark should record
                                latency
                                [boolean] [choices: true, false] [default: true]
```

In most cases, default values for options should be used.

## Generating charts

Depending on the benchmark, the results directory might contain `latency.txt` and `throughput.txt` files.

### Latency by Percentile Distribution

To plot a latency distribution chart use [HdrHistogram Plotter tool][hdr-plotter], selecting one or 
more `latency.txt` files. The file is already in the expected hgrm format.

<img width="640" alt="Latency chart" src="https://user-images.githubusercontent.com/2931196/67383395-64b19980-f58f-11e9-9fb1-832c5ae898ee.png">

### Throughput 

To generate a throughput chart containing different number of concurrent requests use `generate-throughput.sh` shell
 file contained in the `charts` directory.
 
For example, to plot a chart of two throughput benchmark runs which results are in `results/folder1` and 
`results/folder2`, using the maximum concurrent requests of `128`, `256`, `512` and `1024`, the following command 
should be run:

```bash
./charts/generate-throughput.sh folder1 "Sample 1" folder2 "Sample 2" 128,256,512,1024 a-vs-b
``` 

It would generate a file named `a-vs-b.png` in the `results` directory.

<img width="640" alt="Throughput chart" src="https://user-images.githubusercontent.com/2931196/67383326-4b105200-f58f-11e9-950e-70a987387ecf.png">

[driver]: https://github.com/datastax/nodejs-driver
[hdr-plotter]: https://hdrhistogram.github.io/HdrHistogram/plotFiles.html