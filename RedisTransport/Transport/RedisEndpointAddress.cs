using System.Diagnostics;

namespace RedisTransport.Transport;

[DebuggerDisplay("{" + nameof(DebuggerDisplay) + "}")]
public readonly struct RedisEndpointAddress
{
    public const string Scheme = "redis";

    public readonly string Host;
    public readonly int? Port;
    public readonly string Name;
    public readonly AddressType Type;

    public enum AddressType { Queue = 0, Topic = 1 }

    public RedisEndpointAddress(Uri hostAddress, string name, AddressType type = AddressType.Queue)
    {
        Host = hostAddress.Host;
        Port = hostAddress.IsDefaultPort ? null : hostAddress.Port;
        Name = name;
        Type = type;
    }

    public RedisEndpointAddress(Uri hostAddress, Uri address)
    {
        Host = hostAddress.Host;
        Port = hostAddress.IsDefaultPort ? null : hostAddress.Port;

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
            Host = address.Host,
            Port = address.Port ?? -1,
            Path = "/" + address.Name
        };

        if (address.Type == AddressType.Topic)
            builder.Query = "type=topic";

        return builder.Uri;
    }

    Uri DebuggerDisplay => this;
}
