# SheafCore

A high-performance C# library implementing topological matrix evaluation and concurrent edge node stream processing with zero-allocation optimization patterns.

## Overview

SheafCore provides a domain operations layer for linear algebraic validation over network topologies, designed for low-latency distributed systems. The library emphasizes immutability, memory efficiency, and asynchronous processing of edge node streams.

## Core Components

### EdgeNode
An immutable, memory-aligned snapshot of a network node:
- `Id` - Unique identifier for the node
- `DataStream` - Current data value from the node
- `LocalThreshold` - Threshold for local consistency evaluation

### SheafOperations
Domain operations layer handling pure linear algebraic validation:
- **ResolveSystemState** - Processes a read-only span of edge nodes and evaluates their global consistency matrix
  - Returns `SystemResolution` with topology evaluation results
  - Detects kernel anomalies and topological block conflicts
  - Generates target state vectors based on consistency rules

### SystemResolution
Immutable record representing the explicit outcome of topological matrix evaluation:
- `Status` - Human-readable status string
- `IsSuccess` - Boolean flag indicating resolution success
- `StateVector` - Array of computed state values (empty on failure)
- `ErrorMessage` - Optional error details

#### Resolution States
- **GLOBAL_CONSISTENCY_SUCCESS** - All nodes passed consistency checks
- **TOPOLOGICAL_BLOCK_CONFLICT** - Kernel anomaly detected (discrepancies > 0)
- **SYSTEM_FAILURE** - Input validation or processing failure

### IngestionHub
High-performance asynchronous ingestion pipeline for concurrent edge node stream processing:

#### IngestConcurrentStreams
Processes streaming data from multiple concurrent sources with automatic batching:
- Zero-allocation batching via `ReadOnlyMemory<T>`
- Configurable batch size and timeout windows
- Full `CancellationToken` propagation
- Async enumerable yielding `NodeBatch` instances

#### IngestStaticEvents
Ingests fixed collections of events with optional time-window batching:
- Synchronous enumerable for non-streaming scenarios
- Automatic batch flushing on completion

### IngestionConfig
Configuration for ingestion pipeline behavior:
- `BatchSize` - Number of events per batch
- `BatchTimeoutWindow` - Duration before forcing batch closure
- `DefaultLocalThreshold` - Applied to all ingested nodes
- `MaxConcurrentSources` - Concurrency limit (reserved for future use)

## Use Cases

### IoT Sensor Networks
Monitor distributed sensor arrays and validate consistency across edge devices without database row locks:
- Ingest telemetry from hundreds of sensors concurrently
- Evaluate topological consistency across nodes
- Detect anomalies in real-time with sub-millisecond latency
- Zero heap allocation ensures battery-efficient edge device operation

### Autonomous Drone Swarms
Coordinate drone state synchronization with minimal latency overhead:
- Maintain consistency across drone positions and velocities
- Detect conflicts when drone commands create topological inconsistencies
- Process state updates from multiple drones simultaneously
- Eliminate race conditions using immutable matrix operations

### Distributed Financial Systems
Validate transaction consistency across trading nodes:
- Process concurrent order streams from multiple trading venues
- Detect potential causality violations in trade execution
- Ensure global consistency without blocking locks
- Sub-millisecond evaluation for high-frequency trading workloads

### Edge Computing Mesh Networks
Synchronize state across geographically distributed edge compute nodes:
- Evaluate consistency guarantees for distributed computations
- Handle multi-node state transitions without traditional consensus overhead
- Batch ingestion reduces per-event processing latency
- Pure functional architecture eliminates side effects and race conditions

### Real-Time Stream Processing
Validate data quality and consistency in streaming pipelines:
- Ingest continuous data streams with automatic batching
- Detect topological anomalies in streaming telemetry
- Support backpressure through async enumerable patterns
- Zero-allocation paths enable processing of high-throughput data

## Usage Examples

### Basic Topology Evaluation

```csharp
using SheafCore;

// Create edge nodes
EdgeNode[] nodes = new[]
{
    new EdgeNode { Id = 0, DataStream = 45.2, LocalThreshold = 50.0 },
    new EdgeNode { Id = 1, DataStream = 49.8, LocalThreshold = 50.0 },
    new EdgeNode { Id = 2, DataStream = 44.0, LocalThreshold = 50.0 }
};

// Evaluate system state
var result = SheafOperations.ResolveSystemState(nodes);

if (result.IsSuccess)
{
    Console.WriteLine($"Status: {result.Status}");
    Console.WriteLine($"State Vector: [{string.Join(", ", result.StateVector)}]");
}
else
{
    Console.WriteLine($"Resolution failed: {result.Status}");
}
```

### Concurrent Stream Ingestion

```csharp
// Configure ingestion pipeline
var config = new IngestionConfig
{
    BatchSize = 100,
    BatchTimeoutWindow = TimeSpan.FromSeconds(5),
    DefaultLocalThreshold = 50.0,
    MaxConcurrentSources = 10
};

// Process concurrent streams
await foreach (var batch in IngestionHub.IngestConcurrentStreams(
    eventStream, 
    config,
    cancellationToken))
{
    var nodesSpan = IngestionHub.ExtractNodesAsSpan(batch);
    var resolution = SheafOperations.ResolveSystemState(nodesSpan);
    
    // Handle resolution...
}
```

### Static Event Batch Processing

```csharp
IngestionEvent[] events = new[]
{
    new IngestionEvent 
    { 
        SourceId = 1, 
        DataValue = 45.2, 
        SequenceNumber = 1, 
        CaptureTime = DateTime.UtcNow 
    },
    // ... more events
};

foreach (var batch in IngestionHub.IngestStaticEvents(events, config))
{
    var resolution = SheafOperations.ResolveSystemState(
        IngestionHub.ExtractNodesAsSpan(batch));
    // Process batch...
}
```

## Performance Characteristics

### Zero-Allocation Patterns
- `ReadOnlySpan<T>` for stack-based iteration
- `ReadOnlyMemory<T>` for batch storage without heap copying
- Stateless operations on immutable records
- Aggressive inlining for critical paths via `[MethodImpl(MethodImplOptions.AggressiveInlining)]`

### Asynchronous Pipeline
- Native async/await support with `IAsyncEnumerable<T>`
- Full `CancellationToken` propagation through the entire pipeline
- Backpressure support through enumerable consumption patterns

### Scalability
- Designed for distributed edge computing scenarios
- Efficient batching reduces per-event processing overhead
- Memory-aligned data structures for cache locality

## Architecture

```
IngestionHub (Async/Sync Ingestion)
    ↓
NodeBatch (Immutable batch container)
    ↓
SheafOperations (Topology Evaluation)
    ↓
SystemResolution (Result record)
```

## Testing

The library includes comprehensive test coverage via xUnit:

```bash
dotnet test CoreTests.cs
```

### Test Suite
- **ResolveSystemState_MatrixEvaluation_ReturnsExpectedTopology** - Theory-based testing with multiple data scenarios covering:
  - Global consistency success paths
  - Topological block conflict detection
  - State vector correctness validation

## Requirements

- .NET 8.0 or later (C# 12 collection expressions)
- xUnit 2.7+ (for testing)

## Design Principles

1. **Immutability First** - All data structures use records with `init` accessors
2. **Zero-Copy Operations** - Leverage `Span<T>` and `Memory<T>` to eliminate allocations
3. **Async-Native** - Native async/await support throughout the pipeline
4. **Explicit Failure Handling** - All operations return result records with status information
5. **High Performance** - Optimized for sub-millisecond latency in edge scenarios

## Resources & References

### C# Performance & Zero-Allocation Patterns

#### Official Microsoft Documentation
- **[Performance in .NET](https://learn.microsoft.com/en-us/dotnet/standard/performance/)** - Comprehensive guide to performance optimization in .NET
- **[Memory and Spans](https://learn.microsoft.com/en-us/dotnet/standard/memory-and-spans)** - Complete reference for `Span<T>`, `Memory<T>`, and related types
- **[High-performance code with C# and .NET](https://learn.microsoft.com/en-us/dotnet/standard/performance/performance-best-practices)** - Best practices for optimization
- **[ArrayPool<T> Documentation](https://learn.microsoft.com/en-us/dotnet/api/system.buffers.arraypool-1)** - Pooled buffer management

#### Articles & Blogs
- **[Why is Span<T> so limited?](https://devblogs.microsoft.com/dotnet/why-is-span-t-so-limited/)** - Microsoft DevBlogs deep dive on Span limitations and design
- **[Zero-allocation ArrayPool](https://devblogs.microsoft.com/dotnet/zero-allocation-arraypool/)** - ArrayPool best practices for allocation-free code
- **[Announcing .NET 5.0](https://devblogs.microsoft.com/dotnet/announcing-net-5-0/)** - Performance improvements in modern .NET

### Asynchronous Streams & Cancellation

#### Official Microsoft Documentation
- **[Asynchronous Streams (C#)](https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/async/#asynchronous-streams)** - `IAsyncEnumerable<T>` and `await foreach` patterns
- **[EnumeratorCancellation Attribute](https://learn.microsoft.com/en-us/dotnet/api/system.runtime.compilerservices.enumeratorcancellationattribute)** - Proper cancellation in async streams (C# 8.0+)
- **[IAsyncEnumerable<T> Interface](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.iasyncenumerable-1)** - Complete API reference
- **[CancellationToken Documentation](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken)** - Cancellation pattern reference

### Distributed Systems & Consistency Models

#### Recommended Books
- **[Distributed Systems: Principles and Paradigms](https://www.cs.vu.nl/~ast/books/ds5/)** by Andrew S. Tanenbaum and Maarten van Steen - Covers consistency models, vector clocks, and causality
- **[Designing Data-Intensive Applications](https://dataintensive.net/)** by Martin Kleppmann - Excellent chapter on consistency guarantees with visual models
- **[Principles of Distributed Database Systems](https://www2.cs.fiu.edu/~weiss/distributed-books/)** by M. Tamer Özsu and Patrick Valduriez - Formal algebraic approach to consistency

#### Academic Papers & Research
- **[Consistency in Non-Transactional Distributed Storage Systems](https://www.usenix.org/system/files/login/articles/10_020-100_final.pdf)** (Bailis et al., 2013) - Formal evaluation of consistency using dependency graphs
- **[Jepsen Consistency Test Suites](https://jepsen.io/consistency)** - Tools and frameworks for analyzing topological consistency in distributed systems
- **[Wikipedia: Consistency Model](https://en.wikipedia.org/wiki/Consistency_model)** - Comparison table of common consistency models

#### Tools & Frameworks
- **[TLA+ Formal Specification](https://tlaplus.github.io/)** - Formal specification language for verifying consistency properties
- **[Awesome Consistency Models](https://github.com/jepsen-io/consistency-models)** - Curated list of consistency literature and resources

### .NET Ecosystem & Related Technologies

#### Related Libraries
- **[System.Threading.Channels](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels)** - High-performance async channel implementation
- **[System.IO.Pipelines](https://learn.microsoft.com/en-us/dotnet/api/system.io.pipelines)** - Low-allocation streaming with backpressure
- **[Reactive Extensions (Rx.NET)](https://github.com/dotnet/reactive)** - LINQ-style asynchronous data processing
- **[xUnit.net](https://xunit.net/)** - Modern testing framework used in SheafCore tests

#### Additional Resources
- **[.NET Standard Documentation](https://learn.microsoft.com/en-us/dotnet/standard/)** - Cross-platform API consistency
- **[C# 12 Language Features](https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-12)** - Collection expressions and other modern C# features
- **[BenchmarkDotNet](https://benchmarkdotnet.org/)** - Recommended tool for profiling performance-critical code

## License

See LICENSE file for details.

## Contributing

Contributions are welcome. Please ensure:
- All new code includes corresponding unit tests
- Performance characteristics are maintained
- Immutability principles are preserved

For discussions about consistency models, topological evaluation, or performance optimizations, please open an issue or discussion in this repository.
