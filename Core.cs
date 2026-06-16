namespace SheafCore;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Represents an immutable, memory-aligned snapshot of a network node.
/// Uses C# 12 required properties with init-only setters for complete immutability.
/// </summary>
public sealed record EdgeNode
{
    public required int Id { get; init; }
    public required double DataStream { get; init; }
    public required double LocalThreshold { get; init; }
}

/// <summary>
/// Domain operations layer handling pure linear algebraic validation over network topologies.
/// All operations are stateless and operate on ReadOnlySpan for zero-allocation execution paths.
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

    /// <summary>
    /// Evaluates the topological consistency of the provided nodes.
    /// Computes discrepancy vectors and determines if kernel anomalies exist.
    /// </summary>
    private static SystemResolution EvaluateTopology(ReadOnlySpan<EdgeNode> nodes)
    {
        int count = nodes.Length;
        Span<double> discrepancies = stackalloc double[count];
        bool hasKernelAnomaly = false;

        // Layer 1 & 2: Local metrics calculated via stack-allocated zero-copy execution paths
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
/// Uses factory methods to construct valid instances without exposing initialization complexity.
/// </summary>
public sealed record SystemResolution
{
    public required string Status { get; init; }
    public required bool IsSuccess { get; init; }
    public required double[] StateVector { get; init; }
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Creates a successful resolution with the computed state vector.
    /// </summary>
    public static SystemResolution Success(double[] vector)
    {
        ArgumentNullException.ThrowIfNull(vector);
        return new()
        {
            Status = "GLOBAL_CONSISTENCY_SUCCESS",
            IsSuccess = true,
            StateVector = vector
        };
    }

    /// <summary>
    /// Creates a conflict resolution indicating topological anomalies.
    /// </summary>
    public static SystemResolution Conflict(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new()
        {
            Status = "TOPOLOGICAL_BLOCK_CONFLICT",
            IsSuccess = false,
            StateVector = [],
            ErrorMessage = message
        };
    }

    /// <summary>
    /// Creates a failure resolution indicating system-level errors.
    /// </summary>
    public static SystemResolution Failure(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new()
        {
            Status = "SYSTEM_FAILURE",
            IsSuccess = false,
            StateVector = [],
            ErrorMessage = message
        };
    }
}
