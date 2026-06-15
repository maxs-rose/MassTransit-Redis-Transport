# MtRedis — Redis Transport for MassTransit

A custom [MassTransit](https://masstransit.io) transport that uses **Redis Streams** for durable message queues and **Redis Pub/Sub** for low-latency wakeup notifications. It targets scenarios where you already run Redis and want reliable async messaging without adding RabbitMQ or Azure Service Bus to your stack.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [How MassTransit Transports Work](#how-masstransit-transports-work)
3. [Redis Primitives](#redis-primitives)
4. [Architecture](#architecture)
5. [Publish & Send Flow](#publish--send-flow)
6. [Consume Flow](#consume-flow)
7. [Acknowledgment & At-Least-Once Delivery](#acknowledgment--at-least-once-delivery)
8. [Topic Routing](#topic-routing)
9. [Ephemeral Queues](#ephemeral-queues)
10. [Error Handling](#error-handling)
11. [Resilience & Recovery](#resilience--recovery)
12. [Telemetry](#telemetry)
13. [Configuration Reference](#configuration-reference)
14. [Redis Key Schema](#redis-key-schema)

---

## Quick Start

```csharp
builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<PingConsumer>();

    x.UsingRedis((context, cfg) =>
    {
        cfg.Host("localhost:6379");     // or ConfigurationOptions
        cfg.WithPrefix("myapp:");      // optional key prefix
        cfg.ConfigureEndpoints(context);
    });
});
```

That's it. MassTransit auto-discovers your consumers, creates one Redis Stream per queue, and sets up topic subscriptions so `Publish<T>()` fans out to every queue subscribed to `T`.

---

## How MassTransit Transports Work

Understanding this transport is much easier with a mental model of the MT extension points it implements. A full primer is at [masstransit.io/documentation/transports](https://masstransit.io/documentation/transports), but the key abstractions are:

```
IBusFactory / IBusFactoryConfigurator
  └─ IHostConfiguration
       └─ IReceiveEndpointConfiguration  (one per queue)
            └─ ReceiveEndpointContext
                 ├─ IFilter<TContext>     (the pipeline; RedisConsumerFilter lives here)
                 ├─ ISendTransport        (RedisSendTransportContext)
                 └─ IPublishTransport     (wraps ISendTransport for topics)
```

The **configuration phase** (before the bus starts) wires up the object graph. The **runtime phase** (after `host.Run()`) opens connections, starts receive loops, and processes messages.

The transport implements these MT interfaces and base classes:

| MT base/interface | This transport's impl |
|---|---|
| `BusFactoryConfigurator` | `RedisBusFactoryConfigurator` |
| `BaseHostConfiguration<…>` | `RedisHostConfiguration` |
| `ReceiveEndpointConfiguration` | `RedisReceiveEndpointConfiguration` |
| `BaseReceiveEndpointContext` | `QueueRedisReceiveEndpointContext` |
| `ConsumerAgent<string>` | `RedisMessageReceiver` |
| `IFilter<RedisClientContext>` | `RedisConsumerFilter` |
| `BaseSendTransportContext` | `RedisSendTransportContext` |
| `ReceiveLockContext` | `RedisReceiveLockContext` |
| `IErrorTransport` | `RedisErrorTransport` |

For a real-world reference transport you can diff against, see [MassTransit's in-memory transport](https://github.com/MassTransit/MassTransit/tree/develop/src/Transports/MassTransit.ActiveMqTransport) or the [RabbitMQ transport](https://github.com/MassTransit/MassTransit/tree/develop/src/Transports/MassTransit.RabbitMqTransport).

---

## Redis Primitives

Three Redis data structures are used; each has a well-defined role:

### Redis Streams (`mt:q:{name}`)

A [Redis Stream](https://redis.io/docs/latest/develop/data-types/streams/) is an append-only log. This transport uses it as a durable queue:

- `XADD` appends one message as a set of named fields.
- `XREADGROUP` reads up to N messages and places them in the **Pending Entries List (PEL)** for the consumer — they are not lost even if the consumer crashes.
- `XACK` + `XDEL` remove an entry from both the PEL and the stream once it is processed successfully.

### Redis Pub/Sub (`mt:q:{name}:notify`)

[Redis Pub/Sub](https://redis.io/docs/latest/develop/interact/pubsub/) carries **zero-byte wakeup signals**, not messages. When a producer writes to a stream it immediately publishes on the matching notify channel so waiting consumers wake up instead of sleeping until their next polling tick. The message payload never travels over Pub/Sub.

### Redis Hashes (`mt:subs:{topic}`)

Each publish topic has a Hash whose fields are queue names and whose values are `"1"`. This is the fan-out registry:

```
mt:subs:MyApp.Messages.Ping
  ├─ "my-app-ping-consumer"  →  "1"
  └─ "audit-log-consumer"    →  "1"
```

When a message is published to the `Ping` topic, the transport reads this hash with `HKEYS`, then `XADD`s a copy to each subscriber's stream and notifies its channel.

---

## Architecture

### Key Schema

```
mt:q:{queue-name}           Redis Stream   — the durable message queue
mt:q:{queue-name}:notify    Pub/Sub channel — wakeup signal for that queue
mt:subs:{topic-name}        Redis Hash     — {queue-name} → "1" for each subscriber
```

With an optional prefix (`cfg.WithPrefix("myapp:")`), all keys are prefixed: `myapp:mt:q:...`.

### Configuration Object Graph

```
UsingRedis()
└─ RedisBusFactoryConfigurator
    ├─ .Host("…")                       → stores factory for IConnectionMultiplexer
    ├─ .WithPrefix("…")                 → sets RedisKeys.KeyPrefix (global static)
    ├─ .ReceiveEndpoint(…)              → delegates to RedisHostConfiguration
    └─ .ConfigureEndpoints(context)     → auto-discovers consumers
         └─ RedisHostConfiguration
              └─ RedisReceiveEndpointConfiguration   (one per queue)
                   ├─ RedisReceiveSettings           (PrefetchCount, TTL, etc.)
                   └─ Build(host)
                        ├─ QueueRedisReceiveEndpointContext
                        │    ├─ RedisClientContextSupervisor   (manages connection)
                        │    ├─ RedisSendTransportProvider     (for Send<T>)
                        │    └─ RedisPublishTransportProvider  (for Publish<T>)
                        └─ ReceiveTransport<RedisClientContext>
                             └─ pipe: RedisConsumerFilter
```

### Runtime Object Graph

Once started, each receive endpoint runs a single pipe-connected instance:

```
ReceiveTransport<RedisClientContext>
  └─ RedisClientContextSupervisor
       └─ RedisClientContextFactory
            └─ RedisClientContextImpl   (IDatabase + ISubscriber from multiplexer)
                 └─ RedisConsumerFilter.Send()     ← pipeline entry point
                      ├─ RegisterSubscriptionsAsync()
                      │    └─ HSET mt:subs:{topic} {queue} "1"  (per subscribed type)
                      └─ RedisMessageReceiver      (ConsumerAgent)
                           ├─ Consume()            (main loop)
                           └─ RefreshLoop()        (background; subscription health)
```

---

## Publish & Send Flow

### `bus.Publish<Ping>(msg)` — fan-out to all subscribers

```
bus.Publish<Ping>(msg)
  └─ RedisPublishTransportProvider.GetPublishTransport<Ping>()
       └─ RedisSendTransportContext  (entityName = "MyApp.Messages.Ping", type = Topic)
            └─ Send()
                 └─ FanoutToSubscribers()
                      │
                      ├─ HKEYS mt:subs:MyApp.Messages.Ping
                      │    → ["my-app-ping-consumer", "audit-log-consumer"]
                      │
                      ├─ XADD mt:q:my-app-ping-consumer  {all message fields}
                      ├─ PUBLISH mt:q:my-app-ping-consumer:notify  ""
                      ├─ XADD mt:q:audit-log-consumer    {all message fields}
                      └─ PUBLISH mt:q:audit-log-consumer:notify    ""
```

All `XADD` and `PUBLISH` calls for all subscribers fire in parallel (single `Task.WhenAll`).

If the topic hash has no subscribers, the message is **silently dropped** with a debug log — same semantics as RabbitMQ exchanges with no bound queues.

### `bus.Send<T>(address, msg)` — direct to one queue

```
bus.Send(address, msg)
  └─ RedisSendTransportProvider.GetSendTransport(address)
       └─ RedisSendTransportContext  (type = Queue)
            └─ Send()
                 └─ SendToQueue()
                      ├─ XADD mt:q:{queue}  {all message fields}
                      └─ PUBLISH mt:q:{queue}:notify  ""
```

The address scheme is `redis://host/{queue-name}`. Topic addresses use the scheme `topic://host/{topic-name}` (resolved via `RedisEndpointAddress.Type`).

### Message Fields on the Stream

Each `XADD` entry contains flat string fields — no envelope wrapper:

| Stream field | Source |
|---|---|
| `TransportMessageId` | Newly generated `NewId.NextGuid()` |
| `MessageId`, `CorrelationId`, `ConversationId`, `RequestId`, `InitiatorId` | MT send context |
| `Body` | Raw JSON serialized payload (via `UseRawJsonSerializer`) |
| `ContentType` | e.g. `application/json` |
| `MessageType` | Semicolon-separated URNs, e.g. `urn:message:MyApp.Messages:Ping` |
| `SourceAddress`, `DestinationAddress`, `ResponseAddress`, `FaultAddress` | MT send context |
| `SentTime`, `ExpirationTime` | UTC ISO-8601 strings |
| `PartitionKey`, `RoutingKey` | Set by conventions or consumer definition |
| `HeadersJson` | JSON array of any additional custom headers |

---

## Consume Flow

### Startup Sequence

```
RedisConsumerFilter.Send()
  1. RegisterSubscriptionsAsync()
     └─ for each consumed message type T:
          HSET mt:subs:{RedisMessageTypeFormatter.Format(T)}  {queueName}  "1"
     └─ if AutoDeleteOnIdle: EnsureStreamExistsAsync() + ApplyInitialExpiriesAsync()

  2. new RedisMessageReceiver(…)
     └─ Task.Run(Consume)           ← main receive loop
     └─ Task.Run(RefreshLoop)       ← runs if AutoDeleteOnIdle OR has subscriptions

  3. await receiver.Ready           ← blocks until step 4 below completes

  4. Inside Consume():
     a. EnsureConsumerGroup()
        └─ XGROUP CREATE mt:q:{queue} {queue} $ MKSTREAM
           (BUSYGROUP error = group exists, ignored)
     b. SUBSCRIBE mt:q:{queue}:notify   ← Pub/Sub wakeup
     c. SetReady()                       ← unblocks step 3
```

### Receive Loop

```
while (!IsStopping)
  RequestRateAlgorithm.Run(
    fetch:  ReceiveMessages(limit)
    handle: HandleMessage(entry)
  )
```

`RequestRateAlgorithm` is a MassTransit utility that throttles outstanding work to `PrefetchCount` (default 16). It calls `ReceiveMessages` to get a batch, then calls `HandleMessage` for each entry concurrently up to the limit.

**`ReceiveMessages(db, limit, ct)`:**

```
1. TrimExpiredMessages()
   └─ if MessageTimeToLive set:
        XTRIM mt:q:{queue} MINID ~ {cutoff-ms}-0

2. XREADGROUP GROUP {queue} {machine}-{guid} > {limit}
   └─ on NOGROUP error: EnsureConsumerGroup() then return []

3. if entries.Length == 0:
   └─ WaitForNotificationOrTimeout(ct)
        wait for: Pub/Sub wakeup OR PollingInterval (default 1s)
        return []

4. return entries
```

**`HandleMessage(db, entry)`:**

```
1. RedisTransportMessage.FromStreamEntry()   ← decode stream fields

2. if ExpirationTime < now: Complete() (XACK+XDEL), return

3. new RedisReceiveLockContext(…)
   new RedisReceiveContext(…)             ← provides Body + IHeaderProvider to MT

4. Dispatch(entryId, receiveContext, lockContext)
   └─ MT pipeline: deserialize → retry → consumer → saga → …
      on success: lockContext.Complete()   → XACK + XDEL
      on fault:   lockContext.Faulted()    → entry stays in PEL
                  + IErrorTransport.Send() → moves to error stream
```

### Wakeup Notification

The Pub/Sub subscriber and the polling timeout compete on the same `CancellationTokenSource`:

```
MessageHandled() or Pub/Sub message arrives
  └─ Interlocked.Exchange(ref _notifyCts, new CancellationTokenSource()).Cancel()
       └─ WaitForNotificationOrTimeout wakes up immediately
            └─ next XREADGROUP fires without waiting for PollingInterval
```

This means high-throughput queues almost never sleep; the 1-second fallback only matters if the Pub/Sub channel is unavailable.

---

## Acknowledgment & At-Least-Once Delivery

Redis Streams consumer groups implement acknowledgment via the Pending Entries List (PEL). The flow:

```
XREADGROUP …
  → entry moves to PEL for this consumer
  → entry is NOT deleted from stream

Consumer processes message
  ├─ success:  XACK + XDEL   → removed from PEL and stream
  └─ fault:    no-op         → stays in PEL
```

**At-least-once guarantee:** if the consumer crashes after `XREADGROUP` but before `XACK`, the entry remains in the PEL. On the next startup, `XGROUP CREATE … 0-0` is used (not `$`) so the consumer re-reads all unacknowledged entries from `0-0` — this recovers in-flight messages from crashes.

> Note: delivery count tracking (`DeliveryCount > 0` → `isRedelivered` in MT context) is not yet populated from the PEL. Faulted entries in the PEL are currently not auto-reclaimed via `XAUTOCLAIM`; they are recovered on consumer restart.

---

## Topic Routing

### Type → Topic Name Mapping

`RedisMessageTypeFormatter.Format(type)` produces the topic name:

```
MyApp.Messages.Ping  →  "MyApp.Messages.Ping"
```

This is the same string used as:
- The Redis Hash key: `mt:subs:MyApp.Messages.Ping`
- The publish address path: `redis://host/MyApp.Messages.Ping?type=topic`

### Queue Name Mapping

Queue names come from `RedisEndpointNameFormatter`, which uses `DefaultMessageNameFormatter` with custom separators (`::`/`--`/`:`/`-`). `ConfigureEndpoints()` derives the queue name from the consumer type name automatically.

### How Subscriptions Are Registered

When a consumer is configured, `RedisReceiveEndpointBuilder.ConnectConsumePipe<T>()` records `typeof(T)` in `_subscribedTypes`. After the bus starts, `RedisConsumerFilter.RegisterSubscriptionsAsync()` writes one `HSET` per type:

```
HSET mt:subs:MyApp.Messages.Ping   my-app-ping-consumer   "1"
```

Multiple consumer types on the same endpoint produce multiple `HSET` calls on startup, all to different hash keys but all pointing to the same queue name.

### Multi-Consumer Fan-out

Two different services each running `IConsumer<Ping>` each get their own queue. Both queues appear as fields in the same `mt:subs:MyApp.Messages.Ping` hash, so every `Publish<Ping>()` delivers a copy to both:

```
mt:subs:MyApp.Messages.Ping
  service-a-ping-consumer  →  "1"     ← Service A's queue
  service-b-ping-consumer  →  "1"     ← Service B's queue
```

Within a single service, if you want **competing consumers** (multiple instances of the same service), they share one queue and `XREADGROUP` distributes entries across them — each instance gets a unique consumer name (`{Machine}-{Guid}`) but the same consumer group name (the queue name), so Redis load-balances automatically.

---

## Ephemeral Queues

Marking an endpoint as temporary (via `Endpoint(x => x.Temporary = true)` or `AutoDeleteOnIdle`) causes the transport to:

1. **Set TTL on the stream key** (`KeyExpireAsync`) — stream auto-deletes if idle.
2. **Set per-field TTL on topic hash entries** (`HashFieldExpireAsync`, Redis 7.4+) — topic registration auto-expires so publishers don't fan-out to dead queues.
3. **Run a RefreshLoop** — a background loop wakes on `TTL/2` (max 1 minute) to renew both TTLs while the consumer is alive.
4. **Clean up explicitly on stop** — `CleanupAsync` removes all hash fields and deletes the stream key.

The bus endpoint itself uses `AutoDeleteOnIdle = 1 minute` (`Defaults.TemporaryAutoDeleteOnIdle`), so the internal bus queue (used for request/response) disappears after the bus stops.

```
Consumer starts:
  XGROUP CREATE mt:q:temp-queue temp-queue 0-0 MKSTREAM
  HSET mt:subs:Ping temp-queue "1"
  HEXPIRE mt:subs:Ping [temp-queue] {ttl}
  KeyExpire mt:q:temp-queue {ttl}

Every TTL/2 (RefreshLoop):
  HEXPIRE mt:subs:Ping [temp-queue] {ttl}   ← renew
  KeyExpire mt:q:temp-queue {ttl}            ← renew

Consumer stops:
  HDEL mt:subs:Ping temp-queue
  DEL mt:q:temp-queue
```

---

## Error Handling

Failed messages (after all MT retries are exhausted) are moved to an error stream by `RedisErrorTransport`:

```
{queue}  →  {queue}_error
```

e.g. `my-app-ping-consumer` → `my-app-ping-consumer_error`

The error stream entry contains all original message fields plus MT exception headers (`MT-Fault-*`). The error stream is a normal Redis Stream, so you can inspect it with standard Redis tooling:

```bash
XRANGE my-app-ping-consumer_error - +
```

No consumer group is created on the error stream automatically — it is a dead-letter holding area. You can attach a separate endpoint to it if you want programmatic replay.

---

## Resilience & Recovery

### Connection Failure

`RedisHostConfiguration` registers an MT retry policy on the receive transport:

```csharp
Retry.CreatePolicy(x =>
{
    x.Handle<ConnectionException>();
    x.Handle<RedisConnectionException>();
    x.Handle<RedisTimeoutException>();
    x.Exponential(1000, min: 1s, max: 30s, delta: 3s);
});
```

The entire `ReceiveTransport<RedisClientContext>` is restarted with exponential backoff. StackExchange.Redis handles connection multiplexer-level reconnection internally.

### Stream or Consumer Group Deleted

If someone deletes `mt:q:{queue}` from Redis, the next `XREADGROUP` throws `NOGROUP`. The receive loop catches this:

```csharp
catch (RedisServerException ex)
    when (ex.Message.Contains("NOGROUP", …))
{
    // Recreate stream + consumer group (MKSTREAM), then continue
    await EnsureConsumerGroup();
    return [];
}
```

The stream is recreated, the consumer group is re-established with `0-0` (to recover any PEL messages that may have been re-added), and the loop continues without crashing the endpoint.

### Topic Hash Deleted

If `mt:subs:{topic}` is deleted, new publishes stop reaching this queue. The `RefreshLoop` detects this:

```csharp
// EnsureSubscriptionsAsync() runs every refresh tick
foreach (var topic in _subscribedTopics)
{
    if (!await db.HashExistsAsync(RedisKeys.TopicSubscribers(topic), _consumerGroup))
    {
        // Log warning, re-register
        await db.HashSetAsync(RedisKeys.TopicSubscribers(topic), _consumerGroup, "1");
    }
}
```

For permanent queues the refresh interval is `MaxRefreshInterval` (1 minute). Recovery is bounded by that window.

### Pub/Sub Failure

If the Pub/Sub subscription fails (Redis cluster failover, etc.), the consumer logs a warning and falls back to polling at `PollingInterval` (default 1 second). No messages are lost; throughput temporarily degrades to one `XREADGROUP` per second.

---

## Telemetry

The transport integrates with MassTransit's built-in OpenTelemetry support. No additional configuration is needed beyond what MT provides:

```csharp
builder.Services.AddOpenTelemetry()
    .UseOtlpExporter()
    .WithTracing(o => o.AddSource(DiagnosticHeaders.DefaultListenerName));
```

`RedisSendTransportContext` calls `LogContext.StartSendActivity` and `LogContext.StartSendInstrument`, so send spans are created automatically. Consume spans are created by MT's `Dispatch` pipeline.

Trace context is propagated across the wire via MT's standard header conventions — `TraceId` and `SpanId` land in `HeadersJson` on the stream entry and are picked up by `RedisHeaderProvider` on the consumer side.

---

## Configuration Reference

### `cfg.Host(…)` overloads

```csharp
cfg.Host("localhost:6379");                            // connection string
cfg.Host(options);                                     // ConfigurationOptions
cfg.Host(opts => { opts.EndPoints.Add("…"); });        // sync configure
cfg.Host(async opts => { /* await token source */ });  // async configure
cfg.Host("localhost", async opts => { … });            // string + async
```

### `cfg.WithPrefix(string prefix)`

Prepends `prefix` to every Redis key. Useful for multi-tenant deployments or namespace isolation:

```csharp
cfg.WithPrefix("tenant-a:");
// keys become: tenant-a:mt:q:ping-consumer, etc.
```

### `IRedisReceiveEndpointConfigurator` options

Set per-endpoint on `cfg.ReceiveEndpoint(…)` or via `ConsumerDefinition.ConfigureConsumer`:

| Property | Type | Default | Description |
|---|---|---|---|
| `PrefetchCount` | `ushort` | `16` | Max concurrent in-flight messages per consumer instance |
| `PollingInterval` | `TimeSpan` | `1 second` | How long to wait when the queue is empty before polling again |
| `AutoDeleteOnIdle` | `TimeSpan?` | `null` | If set, stream key and topic hash fields get this TTL; refreshed while consumer runs |
| `MessageTimeToLive` | `TimeSpan?` | `null` | If set, `XTRIM MINID` removes messages older than this on each poll |

**Example using `ConsumerDefinition`:**

```csharp
public class PingConsumerDefinition : ConsumerDefinition<PingConsumer>
{
    public PingConsumerDefinition()
    {
        Endpoint(x => x.Temporary = true);   // sets AutoDeleteOnIdle = 1 minute
    }

    protected override void ConfigureConsumer(
        IReceiveEndpointConfigurator endpointConfigurator,
        IConsumerConfigurator<PingConsumer> consumerConfigurator,
        IRegistrationContext context)
    {
        if (endpointConfigurator is IRedisReceiveEndpointConfigurator redis)
        {
            redis.MessageTimeToLive = TimeSpan.FromMinutes(10);
            redis.PrefetchCount = 32;
        }
    }
}
```

---

## Redis Key Schema

```
mt:q:{queue-name}            Stream   Durable message queue
mt:q:{queue-name}:notify     Channel  Pub/Sub wakeup for the queue
mt:subs:{topic-name}         Hash     {queue-name} → "1" for each subscriber
```

### Topic name derivation

`RedisMessageTypeFormatter.Format(typeof(T))` → `"{Namespace}.{Name}"`

```
MyApp.Messages.Ping          → mt:subs:MyApp.Messages.Ping
MyApp.Events.OrderPlaced     → mt:subs:MyApp.Events.OrderPlaced
```

### Queue name derivation

`RedisEndpointNameFormatter` produces kebab-case names from the consumer type, e.g.:

```
PingConsumer        → ping-consumer         → mt:q:ping-consumer
OrderPlacedConsumer → order-placed-consumer → mt:q:order-placed-consumer
```

### Useful Redis CLI commands

```bash
# Inspect a queue
XLEN mt:q:ping-consumer
XRANGE mt:q:ping-consumer - + COUNT 5

# Check pending (unacknowledged) messages
XPENDING mt:q:ping-consumer ping-consumer - + 10

# Check topic subscribers
HGETALL mt:subs:MyApp.Messages.Ping

# Check stream TTL (ephemeral queues)
TTL mt:q:ping-consumer
```
