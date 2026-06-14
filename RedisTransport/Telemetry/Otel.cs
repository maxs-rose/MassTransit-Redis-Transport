using System.Diagnostics;
using MassTransit.Logging;

namespace RedisTransport.Telemetry;

public static class Otel
{
    public const string DefaultListenerName = $"{DiagnosticHeaders.DefaultListenerName}.Redis";
    public const string VerboseListenerName = $"{DiagnosticHeaders.DefaultListenerName}.Redis.Trace";

    internal static readonly ActivitySource ActivitySource = new(DefaultListenerName);
    internal static readonly ActivitySource TraceActivitySource = new(VerboseListenerName);
}