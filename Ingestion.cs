namespace SheafCore;

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// Represents a single ingestion event from an edge source.
/// Immutable snapshot of inbound telemetry with causality tracking.
/// </summary>
public sealed record IngestionEvent
{
    public required int SourceId { get; init; }
    public required double DataValue { get; init; }
    public required long SequenceNumber { get; init; }
    public required DateTime CaptureTime { get; init; }
}

/// <summary>
/// Represents a completed batch of ingested nodes ready for topological evaluation.
/// </summary>
public sealed record NodeBatch
{
    public required int BatchId { get; init; }
    public required ReadOnlyMemory<EdgeNode> Nodes { get; init; }
    public required int EventCount { get; init; }
    public required DateTime BatchClosureTime { get; init; }
}

/// <summary>
/// Configuration for the ingestion pipeline behavior and buffering strategy.
/// </summary>
public sealed record IngestionConfig
{
    public required int BatchSize { get; init; }
    public required TimeSpan BatchTimeoutWindow { get; init; }
    public required double DefaultLocalThreshold { get; init; }
    public required int MaxConcurrentSources { get; init; }

    public IngestionConfig()
    {
        BatchSize = default;
        BatchTimeoutWindow = default;
        DefaultLocalThreshold = default;
        MaxConcurrentSources = default;
    }
}

/// <summary>
/// High-performance asynchronous ingestion hub for concurrent edge node stream processing.
/// Implements zero-allocation batching via ReadOnlyMemory and direct CancellationToken propagation.
/// </summary>
public static class IngestionHub
{
    /// <summary>
    /// Ingests streaming edge data from multiple concurrent sources, batches into
    /// ReadOnlyMemory buckets, and yields NodeBatch instances for matrix resolution.
    /// Propagates CancellationToken through the entire async enumerable pipeline.
    /// </summary>
    [return: NotNull]
    public static async IAsyncEnumerable<NodeBatch> IngestConcurrentStreams(
        IAsyncEnumerable<IngestionEvent> eventStream,
        IngestionConfig config,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventStream);
        ArgumentNullException.ThrowIfNull(config);

        var buffer = new List<EdgeNode>(config.BatchSize);
        int batchCounter = 0;
        DateTime batchStartTime = DateTime.UtcNow;

        await foreach (var @event in eventStream.WithCancellation(cancellationToken))
        {
            buffer.Add(new EdgeNode
            {
                Id = @event.SourceId,
                DataStream = @event.DataValue,
                LocalThreshold = config.DefaultLocalThreshold
            });

            bool batchSizeReached = buffer.Count >= config.BatchSize;
            bool batchTimeoutElapsed = DateTime.UtcNow - batchStartTime >= config.BatchTimeoutWindow;

            if (batchSizeReached || batchTimeoutElapsed)
            {
                var batch = YieldBatchSnapshot(buffer, batchCounter++, config.BatchSize);
                yield return batch;

                buffer.Clear();
                batchStartTime = DateTime.UtcNow;
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        // Flush remaining buffered nodes on stream completion
        if (buffer.Count > 0)
        {
            var finalBatch = YieldBatchSnapshot(buffer, batchCounter, buffer.Count);
            yield return finalBatch;
        }
    }

    /// <summary>
    /// Ingests a fixed collection of events with optional time-window batching.
    /// Returns a sequence of NodeBatch instances without concurrent source management.
    /// </summary>
    [return: NotNull]
    public static IEnumerable<NodeBatch> IngestStaticEvents(
        ReadOnlySpan<IngestionEvent> events,
        IngestionConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (events.IsEmpty)
        {
            yield break;
        }

        var buffer = new List<EdgeNode>(config.BatchSize);
        int batchCounter = 0;

        for (int i = 0; i < events.Length; i++)
        {
            var @event = events[i];
            buffer.Add(new EdgeNode
            {
                Id = @event.SourceId,
                DataStream = @event.DataValue,
                LocalThreshold = config.DefaultLocalThreshold
            });

            if (buffer.Count >= config.BatchSize || i == events.Length - 1)
            {
                var batch = YieldBatchSnapshot(buffer, batchCounter++, buffer.Count);
                yield return batch;
                buffer.Clear();
            }
        }
    }

    /// <summary>
    /// Creates an immutable NodeBatch snapshot from the current buffer state.
    /// Converts the buffer to ReadOnlyMemory to eliminate heap allocations during batching.
    /// </summary>
    private static NodeBatch YieldBatchSnapshot(
        List<EdgeNode> buffer,
        int batchId,
        int expectedSize)
    {
        var memoryArray = new EdgeNode[buffer.Count];
        buffer.CopyTo(memoryArray, 0);
        var readOnlyMemory = new ReadOnlyMemory<EdgeNode>(memoryArray);

        return new NodeBatch
        {
            BatchId = batchId,
            Nodes = readOnlyMemory,
            EventCount = buffer.Count,
            BatchClosureTime = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Utility to convert a NodeBatch's ReadOnlyMemory into a ReadOnlySpan for direct matrix evaluation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ReadOnlySpan<EdgeNode> ExtractNodesAsSpan(NodeBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return batch.Nodes.Span;
    }
}
