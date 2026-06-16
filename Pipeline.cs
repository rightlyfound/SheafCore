namespace SheafCore;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// End-to-end ingestion and synchronization pipeline orchestrator.
/// Coordinates batching, resolution, and reconciliation in a single coherent workflow.
/// </summary>
public static class SheafPipeline
{
    /// <summary>
    /// Processes a stream of batched edge nodes through the complete synchronization pipeline.
    /// Each batch is resolved for topological consistency and reconciled asynchronously.
    /// </summary>
    [return: NotNull]
    public static async Task<PipelineResult> ProcessAsync(
        IAsyncEnumerable<NodeBatch> batchStream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batchStream);

        int batchesProcessed = 0;
        int totalNodesProcessed = 0;
        int failedBatches = 0;
        var errors = new List<string>();

        await foreach (var batch in batchStream.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var nodesSpan = IngestionHub.ExtractNodesAsSpan(batch);
            var syncResult = await TopologicalSync.ReconcileAsync(nodesSpan, cancellationToken);

            if (syncResult.IsSuccessful)
            {
                totalNodesProcessed += syncResult.NodesProcessed;
                batchesProcessed++;
            }
            else
            {
                failedBatches++;
                if (syncResult.ErrorMessage != null)
                {
                    errors.Add($"Batch {batch.BatchId}: {syncResult.ErrorMessage}");
                }
            }
        }

        return new PipelineResult
        {
            TotalBatchesProcessed = batchesProcessed,
            TotalNodesProcessed = totalNodesProcessed,
            FailedBatches = failedBatches,
            Errors = errors
        };
    }
}

/// <summary>
/// Immutable result of a complete pipeline execution.
/// </summary>
public sealed record PipelineResult
{
    public required int TotalBatchesProcessed { get; init; }
    public required int TotalNodesProcessed { get; init; }
    public required int FailedBatches { get; init; }
    public required List<string> Errors { get; init; }

    public bool IsSuccessful => FailedBatches == 0 && Errors.Count == 0;
}
