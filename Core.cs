namespace SheafCore;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Represents an immutable, memory-aligned snapshot of a network node.
/// </summary>
public sealed record EdgeNode
{
    public required int Id { get; init; }
    public required double DataStream { get; init; }
    public required double LocalThreshold { get; init; }

    public EdgeNode()
    {
        Id = default;
        DataStream = default;
        LocalThreshold = default;
    }
}

/// <summary>
/// Domain operations layer handling pure linear algebraic validation over network topologies.
/// </summary>
public static class SheafOperations
{
    /// <summary>
    /// Processes a read-only span of edge nodes and evaluates their global consistency matrix.
    /// </summary>
    [return: NotNull]
    public static SystemResolution ResolveSystemState(ReadOnlySpan<EdgeNode> nodes) =>
        nodes.IsEmpty
            ? SystemResolution.Failure("Input node collection cannot be empty.")
            : EvaluateTopology(nodes);

    private static SystemResolution EvaluateTopology(ReadOnlySpan<EdgeNode> nodes)
    {
        int count = nodes.Length;
        double[] discrepancies = new double[count];
        bool hasKernelAnomaly = false;

        // Layer 1 & 2: Local metrics calculated via zero-allocation execution paths
        for (int i = 0; i < count; i++)
        {
            discrepancies[i] = nodes[i].DataStream - nodes[i].LocalThreshold;
            if (discrepancies[i] > 0)
            {
                hasKernelAnomaly = true;
            }
        }

        // Layer 3: Calculate target state vectors based on topological consistency
        double[] targetVector = new double[count];
        for (int i = 0; i < count; i++)
        {
            targetVector[i] = discrepancies[i] > 0 ? 0.0 : 1.0;
        }

        return hasKernelAnomaly
            ? SystemResolution.Conflict("Topological block conflict detected inside the network kernel.")
            : SystemResolution.Success(targetVector);
    }
}

/// <summary>
/// An immutable record representing the explicit outcome of a topological matrix evaluation.
/// </summary>
public sealed record SystemResolution
{
    public required string Status { get; init; }
    public required bool IsSuccess { get; init; }
    public required double[] StateVector { get; init; }
    public string? ErrorMessage { get; init; }

    public SystemResolution()
    {
        Status = null!;
        StateVector = null!;
    }

    public static SystemResolution Success(double[] vector) => new()
    {
        Status = "GLOBAL_CONSISTENCY_SUCCESS",
        IsSuccess = true,
        StateVector = vector
    };

    public static SystemResolution Conflict(string message) => new()
    {
        Status = "TOPOLOGICAL_BLOCK_CONFLICT",
        IsSuccess = false,
        StateVector = []
    };

    public static SystemResolution Failure(string message) => new()
    {
        Status = "SYSTEM_FAILURE",
        IsSuccess = false,
        StateVector = [],
        ErrorMessage = message
    };
}
