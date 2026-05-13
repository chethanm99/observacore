# Architecture

The following sections of this document, explains the architecture of Observacore diving deep into every ascpect of the project.

## Lifecycle

This section gives a brief idea about the flow of work and role of each of component at each stage of Observacore.

### Receiver Generates Telemetry

- The lifecycle of ObservaCore begins with the in-memory receiver generating telemetry data. Currently, telemetry is generated internally within the system. Additional receivers such as HTTP Receiver and Prometheus Receiver will be introduced in future phases of the project.

- Each telemetry metric contains:
  - a metric name, randomly selected from a predefined list in the config file.
  - a metric value, randomly generated based on configured threshold range.

- These raw metrics are forwarded into the pipeline through channels for further processing and filtering.

---

### Worker Pool Processes the Data

- Once raw metrics are emitted by the receiver, they are processed based on the configured processor type and processing rules.

- Currently, ObservaCore uses a CPU Filter Processor. Future phases will introduce additional processors such as:
  - Deduplication Processor
  - Enrichment Processor
  - Sampling Processor

- The CPU Filter Processor currently allows only:
  - metrics with the name `"CPU"`
  - metrics whose value satisfies the configured threshold

- Each processor contains a worker pool where workers independently consume metrics from channels and process them concurrently.

- The number of workers and channel buffer sizes are configurable, allowing the pipeline to scale based on workload and throughput requirements.

- After processing, valid metrics are forwarded to the batching stage.

---

### Batching the Telemetry Data

- Processed telemetry data is grouped into batches based on configurable batch sizes and flush intervals.

- Batching improves efficiency by reducing the number of individual export operations and network calls.

- Currently, batching is performed using:
  - size-based batching
  - interval-based flushing

- Future phases will introduce more advanced batching strategies based on:
  - traffic patterns
  - pipeline load
  - adaptive runtime conditions

---

### Retrying Failed Batches

- Batches that fail during export are forwarded to a retry queue for reprocessing.

- The retry mechanism attempts to resend failed batches until the configured maximum retry limit is reached. Once the retry limit is exceeded, batches are dropped to prevent infinite retry loops and resource exhaustion.

- Currently, ObservaCore uses an in-memory retry queue. Future phases will introduce persistent retry storage to ensure failed telemetry data is not lost during crashes or restarts.

---

## Design Desicions

This section of Observacore explains about the design decisions used in these project.

## Why interfaces?

- Using interfaces give a flexibility to implement various plugins and extensions without having to change the code.

- This setup helps Observacore with testing by setting mock exporter or receiver to test the behaviour of the project.

- It is useful when users need to swap between the components, let's say if they want to swap between file exporter and kafka exporter, they can just do it by changing the config file.

## Why factories?

- Factories act as a centralized unit of logic building. It acts as a middleware to implement and build the pipeline configuration into a actual operational pipeline.

- Factories come up with several checkpoints while building each component. It helps in "failing fast" and stop if a particular condition is not met.

## Why config driven?

- The config driven setup of Observacore, keeps the components loosely coupled and open for expansion.

- This main benefit of this setup is that the values are not hard-coded, they can be changed by changing the config file.

- Any feature can be implemented by just implementing the interface method and adding one case to the factory.

## Why batching?

- Batching helps in sending the processed requests in group or a batch, than each request being sent to the exporter, which creates a network overhead.

- Sending the processed metrics in batches, reduces the CPU resource consumption, and enhancing the efficiency of the system.

- In observacore, batcher gets triggered by dual trigger mechansim:
  - `Size based trigger`: In this, metrics are flushed once a specific threshold is met.

  - `Time based trigger`: In this batches are flushed after every specific interval of time.
