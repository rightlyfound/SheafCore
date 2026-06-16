namespace SheafCore.Tests;

using Xunit;
using Xunit.Abstractions;

/// <summary>
/// Comprehensive test suite for SheafCore domain operations.
/// Uses xUnit v3 with TheoryData matrices for exhaustive coverage.
/// </summary>
public sealed class SheafCoreTests(ITestOutputHelper output)
{
    /// <summary>
    /// Test data matrix for matrix evaluation verification.
    /// Each row tests a specific topological consistency scenario.
    /// </summary>
    public static TheoryData<double[], double, bool, string> SensorSuiteTestData =>
    [
        ([45.2, 49.8, 44.0], 50.0, true, "GLOBAL_CONSISTENCY_SUCCESS"),
        ([45.2, 51.1, 44.0], 50.0, false, "TOPOLOGICAL_BLOCK_CONFLICT"),
        ([50.0, 50.0, 50.0], 50.0, true, "GLOBAL_CONSISTENCY_SUCCESS"),
        ([51.0, 51.0, 51.0], 50.0, false, "TOPOLOGICAL_BLOCK_CONFLICT")
    ];

    /// <summary>
    /// Verifies that SheafOperations.ResolveSystemState correctly evaluates topological consistency.
    /// </summary>
    [Theory]
    [MemberData(nameof(SensorSuiteTestData))]
    public async Task ResolveSystemState_MatrixEvaluation_ReturnsExpectedTopology(
        double[] streams,
        double threshold,
        bool expectedSuccess,
        string expectedStatus)
    {
        output.WriteLine($"Testing {nameof(SheafOperations.ResolveSystemState)} with {streams.Length} nodes");
        
        // Arrange: Construct edge nodes from test data
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
        await Task.Delay(1, cancellationToken);

        // Act: Execute topological resolution
        var result = SheafOperations.ResolveSystemState(nodesSpan);

        // Assert: Verify expected outcomes
        Assert.NotNull(result);
        Assert.Equal(expectedStatus, result.Status);
        Assert.Equal(expectedSuccess, result.IsSuccess);

        if (result.IsSuccess)
        {
            Assert.NotEmpty(result.StateVector);
            Assert.All(result.StateVector, val => Assert.Equal(1.0, val));
        }
        else
        {
            Assert.Empty(result.StateVector);
            Assert.NotNull(result.ErrorMessage);
        }

        output.WriteLine($"Completed test: {expectedStatus}");
    }

    /// <summary>
    /// Verifies that empty node collections are properly rejected.
    /// </summary>
    [Fact]
    public void ResolveSystemState_EmptyCollection_ReturnsFailure()
    {
        // Act
        var result = SheafOperations.ResolveSystemState(ReadOnlySpan<EdgeNode>.Empty);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal("SYSTEM_FAILURE", result.Status);
        Assert.NotNull(result.ErrorMessage);
        Assert.Empty(result.StateVector);

        output.WriteLine("Empty collection test passed");
    }

    /// <summary>
    /// Verifies that SystemResolution factory methods create valid instances.
    /// </summary>
    [Fact]
    public void SystemResolution_Factories_ProduceValidResults()
    {
        // Arrange
        var vector = new double[] { 1.0, 0.0, 1.0 };

        // Act
        var success = SystemResolution.Success(vector);
        var conflict = SystemResolution.Conflict("Test conflict");
        var failure = SystemResolution.Failure("Test failure");

        // Assert
        Assert.True(success.IsSuccess);
        Assert.NotEmpty(success.StateVector);

        Assert.False(conflict.IsSuccess);
        Assert.Empty(conflict.StateVector);
        Assert.Equal("TOPOLOGICAL_BLOCK_CONFLICT", conflict.Status);

        Assert.False(failure.IsSuccess);
        Assert.Empty(failure.StateVector);
        Assert.Equal("SYSTEM_FAILURE", failure.Status);

        output.WriteLine("SystemResolution factory tests passed");
    }
}
