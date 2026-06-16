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

## License

See LICENSE file for details.

## Contributing

Contributions are welcome. Please ensure:
- All new code includes corresponding unit tests
- Performance characteristics are maintained
- Immutability principles are preserved
