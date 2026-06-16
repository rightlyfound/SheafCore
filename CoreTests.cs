namespace SheafCore.Tests;

using Xunit;
using Xunit.Abstractions;

public sealed class SheafCoreTests(ITestOutputHelper output)
{
    // High-performance matrix test suite data using C# 12 collection expressions
    public static TheoryData<double[], double, bool, string> SensorSuiteTestData => [
        ([45.2, 49.8, 44.0], 50.0, true, "GLOBAL_CONSISTENCY_SUCCESS"),
        ([45.2, 51.1, 44.0], 50.0, false, "TOPOLOGICAL_BLOCK_CONFLICT")
    ];

    [Theory]
    [MemberData(nameof(SensorSuiteTestData))]
    public async Task ResolveSystemState_MatrixEvaluation_ReturnsExpectedTopology(
        double[] streams,
        double threshold,
        bool expectedSuccess,
        string expectedStatus)
    {
        // Arrange
        output.WriteLine($"Starting test verification via context channel for {nameof(SheafOperations.ResolveSystemState)}");
        
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

        // ReadOnlySpan window allocation optimization
        ReadOnlySpan<EdgeNode> nodesSpan = nodeArray;

        // Flow async token using modern xUnit v3 TestContext rules
        var cancellationToken = TestContext.Current.CancellationToken;
        await Task.Delay(1, cancellationToken);

        // Act
        var result = SheafOperations.ResolveSystemState(nodesSpan);

        // Assert
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
        }
    }
}
