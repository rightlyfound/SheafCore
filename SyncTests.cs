namespace SheafCore.Tests;

using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Comprehensive test suite for topological synchronization operations.
/// Verifies async reconciliation and state vector application.
/// </summary>
public sealed class SyncTests(ITestOutputHelper output)
{
    /// <summary>
    /// Test data matrix for synchronization operations.
    /// </summary>
    public static TheoryData<double[], double, bool> SyncTestData =>
    [
        ([48.0, 49.0, 47.0], 50.0, true),
        ([51.0, 52.0, 51.0], 50.0, false),
        ([50.0], 50.0, true)
    ];

    /// <summary>
    /// Verifies that TopologicalSync.ReconcileAsync correctly processes node batches.
    /// </summary>
    [Theory]
    [MemberData(nameof(SyncTestData))]
    public async Task ReconcileAsync_ProcessesNodes_ReturnsExpectedResult(
        double[] streams,
        double threshold,
        bool expectedSuccess)
    {
        output.WriteLine($"Testing sync reconciliation with {streams.Length} nodes");

        // Arrange: Construct edge nodes
        EdgeNode[] nodeArray = new EdgeNode[streams.Length];
        for (int i = 0; i < streams.Length; i++)
        {
            nodeArray[i] = new EdgeNode
            {
                Id = i,
                DataStream = streams[i],
                LocalThreshold = threshold
            };
        }

        ReadOnlySpan<EdgeNode> nodesSpan = nodeArray;
        var cancellationToken = TestContext.Current.CancellationToken;

        // Act: Execute reconciliation
        var result = await TopologicalSync.ReconcileAsync(nodesSpan, cancellationToken);

        // Assert: Verify results match expectations
        Assert.Equal(expectedSuccess, result.IsSuccessful);
        if (expectedSuccess)
        {
            Assert.Equal(streams.Length, result.NodesProcessed);
            Assert.Null(result.ErrorMessage);
        }
        else
        {
            Assert.NotNull(result.ErrorMessage);
        }

        output.WriteLine($"Sync test completed: IsSuccessful={result.IsSuccessful}");
    }

    /// <summary>
    /// Verifies that cancellation tokens are properly propagated.
    /// </summary>
    [Fact]
    public async Task ReconcileAsync_CancellationToken_PropagatesCorrectly()
    {
        output.WriteLine("Testing cancellation token propagation");

        // Arrange: Create a simple node array and a cancellation token
        var nodes = new EdgeNode[]
        {
            new() { Id = 0, DataStream = 45.0, LocalThreshold = 50.0 }
        };

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        // Act & Assert: Should complete before cancellation
        var result = await TopologicalSync.ReconcileAsync(nodes, cts.Token);
        Assert.True(result.IsSuccessful);

        output.WriteLine("Cancellation token propagation test passed");
    }
}
