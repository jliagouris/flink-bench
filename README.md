# Flink-microbenchmarks

## Build instructions
- Required software:
  - Apache Maven 3.x.
  - Java JDK 8
- Run `mvn clean package` inside the `map-chain` directory. If the command succeeds you will find the application jar in `target/map-chain-0.1.jar`

## Flink installation and setup
- Download and extract [Apache Flink 1.7.2.](https://www.apache.org/dyn/closer.lua/flink/flink-1.7.2/flink-1.7.2-bin-scala_2.11.tgz).
- Copy the `opt/flink-metrics-slf4j-1.7.1.jar` file inside the `lib` folder.
- Add the following lines in `conf/flink-conf.yaml`:
  - `metrics.reporter.slf4j.class: org.apache.flink.metrics.slf4j.Slf4jReporter`
  - `metrics.reporter.slf4j.interval: 30 SECONDS` (metrics will be reported every 30s)
  
## Run the job
- Start Flink with `./bin/start-cluster.sh`. Visit `http://localhost:8081` to make sure it is running and check your configuration.
- Submit the job: `./bin/flink run /path-to-your-jar/map-chain-0.1.jar [parameters]`, where the following parameters can be specified with `--param-name param-value`:
  - `chain-length`: number of map operators, DEFAULT: 5
  - `enable-chaining`: whether or not to execute all mappers in the same thread, DEFAULT: true
  - `source-rate`: integers per second pushed by the source, DEFAULT: 50000
  - `source-max-events`: maximum number of generated numbers, DEFAULT: 1000000
  - `source-parallelism`: the source parallelism, DEFAULT: 1
  - `latency-interval`: how often to measure latency, DEFAULT: 5000 (ms)
  
  ## Metrics collection
  The metrics will be written in the task manager's log inside the `log` folder, e.g. `*-taskexecutor-0.log`. You will need to extract the following:
  - `numRecordsInPerSecond`
  - `numRecordsOutPerSecond`
  - `%latency%` or histogram reports
