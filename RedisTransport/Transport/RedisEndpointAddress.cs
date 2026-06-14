using System.Diagnostics;

namespace RedisTransport.Transport;

[DebuggerDisplay("{" + nameof(DebuggerDisplay) + "}")]
internal readonly struct RedisEndpointAddress
{
    private const string Scheme = "redis";

    private readonly string _host;
    private readonly int? _port;

    public readonly string Name;
    public readonly AddressType Type;

    public enum AddressType
    {
        Queue = 0,
        Topic = 1
    }

    public RedisEndpointAddress(Uri hostAddress, string name, AddressType type = AddressType.Queue)
    {
        _host = hostAddress.Host;
        _port = hostAddress.IsDefaultPort ? null : hostAddress.Port;
        Name = name;
        Type = type;
    }

    public RedisEndpointAddress(Uri hostAddress, Uri address)
    {
        _host = hostAddress.Host;
        _port = hostAddress.IsDefaultPort ? null : hostAddress.Port;

        var path = address.AbsolutePath.TrimStart('/');
        Name = string.IsNullOrEmpty(path) ? throw new ArgumentException("Endpoint name required", nameof(address)) : path;
        Type = address.Scheme.Equals("topic", StringComparison.OrdinalIgnoreCase)
            ? AddressType.Topic
            : AddressType.Queue;
    }

    public static implicit operator Uri(in RedisEndpointAddress address)
    {
        var builder = new UriBuilder
        {
            Scheme = Scheme,
            Host = address._host,
            Port = address._port ?? -1,
            Path = "/" + address.Name
        };

        if (address.Type == AddressType.Topic)
            builder.Query = "type=topic";

        return builder.Uri;
    }

    private Uri DebuggerDisplay => this;
}