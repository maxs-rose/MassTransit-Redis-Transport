namespace RedisTransport.Configuration;

internal static class RedisMessageTypeFormatter
{
    public static string Format(Type type)
    {
        var ns = type.Namespace ?? "";
        return string.IsNullOrEmpty(ns) ? type.Name : $"{ns}.{type.Name}";
    }
}