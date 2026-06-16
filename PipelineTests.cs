namespace SheafCore.Tests;

using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Comprehensive test suite for end-to-end pipeline operations.
/// Verifies batch ingestion, resolution, and reconciliation workflows.
/// </summary>
public sealed class PipelineTests(ITestOutputHelper output)
{
    /// <summary>
    /// Generates a stream of test batches for pipeline validation.
    /// </summary>
    private static async IAsyncEnumerable<NodeBatch> GenerateTestBatches(int batchCount)
    {
        for (int b = 0; b < batchCount; b++)
        {
            var nodes = new EdgeNode[3]
            {
                new() { Id = b * 3, DataStream = 45.0, LocalThreshold = 50.0 },
                new() { Id = b * 3 + 1, DataStream = 49.0, LocalThreshold = 50.0 },
                new() { Id = b * 3 + 2, DataStream = 48.0, LocalThreshold = 50.0 }
            };

            var batch = new NodeBatch
            {
                BatchId = b,
                Nodes = new ReadOnlyMemory<EdgeNode>(nodes),
                EventCount = nodes.Length,
                BatchClosureTime = DateTime.UtcNow
            };

            yield return batch;
            await Task.Delay(5);
        }
    }

    /// <summary>
    /// Verifies that the pipeline successfully processes multiple batches.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task ProcessAsync_MultipleBatches_ReturnsAggregatedResult(int batchCount)
    {
        output.WriteLine($"Testing pipeline with {batchCount} batches");

        // Act: Process batches through the pipeline
        var result = await SheafPipeline.ProcessAsync(
            GenerateTestBatches(batchCount),
            TestContext.Current.CancellationToken);

        // Assert: Verify aggregated results
        Assert.Equal(batchCount, result.TotalBatchesProcessed);
        Assert.Equal(batchCount * 3, result.TotalNodesProcessed);
        Assert.Equal(0, result.FailedBatches);
        Assert.True(result.IsSuccessful);

        output.WriteLine($"Pipeline processed {result.TotalNodesProcessed} nodes across {result.TotalBatchesProcessed} batches");
    }
}
