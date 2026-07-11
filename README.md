[![.net build main and dev](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet.yml/badge.svg)](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet.yml)
[![.net package publish to nuget](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet-beta-release.yml/badge.svg)](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet-beta-release.yml)

# Why Use KafkaDataConsumerService and KafkaDataProducerService?
## Simplified Kafka Integration
+ **Ease of Use**: These services abstract the complexities of directly interacting with Kafka APIs, offering a more straightforward and user-friendly way to implement Kafka producers and consumers in .NET applications.
+ **Configurable**: With easy-to-set configuration options in '**appsettings.json**', these services allow for quick adjustments to Kafka settings without diving deep into code changes.
## Enhanced Message Processing
+ **Asynchronous Processing**: Built-in support for asynchronous processing helps in building responsive and performant applications.
+ **Customizable Message Handling**: The ability to define custom message processing logic (via '**ProcessMessageAsync**') provides flexibility to tailor the behavior based on specific business requirements.
## Robust Error Handling and Reporting
+ **Error Handling**: The services include robust error handling mechanisms, reducing the chances of unhandled exceptions and improving application stability.
+ **Callback Functions**: '**OnProduceReportCallback**' and '**OnProduceExceptionCallback**' in the producer service allow for detailed monitoring and logging of message delivery status and handling of production exceptions.
## Scalability and Performance
+ **Concurrent Processing**: Utilization of '**ConcurrentQueue**' and efficient management of threads ensure that the services are capable of handling high-throughput scenarios.
+ **Manual Offset Management**: The consumer service's ability to manually commit offsets (controllable via '**AutoCommit**' configuration) provides greater control over message processing, ensuring that messages are not marked as "consumed" until they are successfully processed.
## Versatility in Message Handling
+ **Flexibility with Message Queue**: In the consumer service, if '**ProcessMessageAsync**' is not set, messages are queued, allowing for alternative processing strategies. Messages can be processed at a later time using the '**TryGetMessage**' method.
+ **Adaptability**: Suitable for various use cases, from simple message passing to complex, high-throughput streaming applications.
## Clean and Maintainable Code
+ **Modularity**: Services are designed to be modular and easily integrable into existing applications.
+ **Encapsulation**: Encapsulation of Kafka-related functionalities within services keeps the codebase clean and maintainable.

# Kafka Services Integration Guide

This guide provides instructions on integrating and using the '**KafkaDataConsumerService**' and '**KafkaDataProducerService**' in a .NET application.

## Configuration
### appsettings.json

Configure your Kafka services in '**appsettings.json**' as follows:

```json
{
  "KafkaProducerConfig": {
    "BootstrapServers": "localhost:9092",
    "MessageMaxBytes": 1000000,
    "RequestTimeoutMs": 5000,
    "SocketTimeoutMs": 60000,
    "CompressionType": "gzip",
    "CompressionLevel": 5,
    "Acks": "all",
    "EnableIdempotence": true,
    "MaxInFlight": 5
  },
  "KafkaConsumerConfig": {
    "BootstrapServers": "localhost:9092",
    "MessageMaxBytes": 1000000,
    "SocketTimeoutMs": 60000,
    "GroupId": "my-consumer-group",
    "MakeGroupUnique": false,
    "AutoCommit": true,
    "AutoOffsetReset": "Error",
    "Topics": ["topic1", "topic2"]
  },
  "KafkaTopicManager": {
    "BootstrapServers": "localhost:9092",
    "EnsureExistTopics": ["orders.events.v1"],
    "NumPartitionsDefault": 3,
    "ReplicationFactorDefault": 3,
    "ReconcileExistingTopicConfigs": true,
    "TopicConfigsDefault": {
      "min.insync.replicas": "2",
      "unclean.leader.election.enable": "false"
    },
    "EnsureExistTopicSpecifications": [
      {
        "Name": "orders.compacted.v1",
        "NumPartitions": 6,
        "ReplicationFactor": 3,
        "Configs": {
          "cleanup.policy": "compact"
        }
      }
    ]
  }
}
```

## KafkaProducerConfig and KafkaConsumerConfig

These sections define the settings for your Kafka producer and consumer, including bootstrap servers, timeouts, compression settings, and consumer group configuration.

### KafkaProducerConfig Properties

+ **BootstrapServers**: A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a "bootstrap" Kafka cluster that a Kafka client connects to initially to bootstrap itself. They are in the format host1:port1,host2:port2,....
+ **MessageMaxBytes**: The maximum size of the message that the producer can send. It controls the maximum size of a message that can be produced.
+ **ReceiveMessageMaxBytes**: The maximum size of a message that the producer can receive in response from the broker.
+ **MessageTimeoutMs**: The time the producer will wait for a request to complete before timing out.
+ **RequestTimeoutMs**: The maximum time in milliseconds the broker is allowed to process the request.
+ **SocketTimeoutMs**: The timeout for network requests. The time to wait for a network operation to complete.
+ **SocketKeepaliveEnable**: Enables TCP keep-alive on the socket connecting to the Kafka broker. It keeps the connection active even if no data is being transferred.
+ **CompressionType**: Specifies the compression codec to use for compressing message sets. Common values are none, gzip, snappy, and lz4.
+ **CompressionLevel**: Represents the compression level for compressed messages. The higher the level, the better the compression.
+ **Acks**: Configures the required Kafka acknowledgements. Use `all` for durable delivery.
+ **EnableIdempotence**: Enables producer idempotence so retries do not create duplicate records within a producer session.
+ **MaxInFlight**: Limits concurrent in-flight requests per broker connection. Keep this at five or lower when idempotence is enabled.

`Acks`, `EnableIdempotence`, and `MaxInFlight` are opt-in for backward compatibility. When they are omitted, the package leaves the underlying Confluent.Kafka defaults unchanged. For durable publication, configure all three values as shown above.

### KafkaConsumerConfig Properties

+ **BootstrapServers**: Similar to the producer, it's a list of Kafka broker addresses.
+ **MessageMaxBytes**: Controls the maximum size of a fetch message. Helps to control memory usage of the consumer.
+ **SocketTimeoutMs**: The timeout for network requests.
+ **GroupId**: The name of the consumer group this consumer belongs to. Consumer groups allow a group of consumers to cooperate in consuming the messages.
+ **MakeGroupUnique**: When set to true, appends a unique identifier (timestamp) to the GroupId, creating a unique consumer group on every run.
+ **AutoCommit**: If set to true, the consumer's offset will be periodically committed in the background.
+ **AutoOffsetReset**: Optional Confluent.Kafka policy (`Earliest`, `Latest`, or `Error`) used when no initial offset exists or the committed offset is unavailable. Omit it to preserve the existing client default. Archival consumers should prefer `Error` so retention loss fails visibly instead of silently skipping data.
+ **Topics**: An array of topics this consumer should subscribe to.

### Kafka topic creation

`KafkaTopicManager` preserves the legacy `EnsureExistTopics` string-array contract. Missing legacy topics use `NumPartitionsDefault`, `ReplicationFactorDefault`, and the optional `TopicConfigsDefault` values.

`EnsureExistTopicSpecifications` is an optional richer contract for topics that need their own partition count, replication factor, or Kafka topic configs. Omitted per-topic partition and replication values fall back to the section defaults. Per-topic `Configs` override matching `TopicConfigsDefault` entries. A detailed specification with the same name as a legacy string entry replaces that legacy definition, which supports an incremental configuration migration without creating the topic twice.

The manager validates topic definitions before contacting Kafka. Partition count and replication factor must be positive, `min.insync.replicas` must be a positive integer no greater than the resolved replication factor, `unclean.leader.election.enable` must be a boolean, and duplicate or conflicting detailed specifications are rejected.

`ReconcileExistingTopicConfigs` is optional and defaults to `false`, preserving the legacy creation-only behavior. When enabled, the manager describes each existing topic, compares only the supported durability settings, and uses Kafka's incremental alter-config API with `SET` operations only for drifted values. Repeated startup with matching values performs no alteration.

Existing-topic reconciliation currently supports only:

- `min.insync.replicas`
- `unclean.leader.election.enable`

Other entries, such as `cleanup.policy`, still apply when a topic is created but are not changed on an existing topic. Before reconciling `min.insync.replicas`, the manager validates it against the existing topic's live replication factor. It never increases partitions or changes replica assignments. Enabling reconciliation requires Kafka describe-config and incremental-alter-config permissions. Disabling it later stops future reconciliation but does not roll back dynamic topic values already applied.

## Implementing Services
### KafkaDataProducerService

Dependency Injection
In your '**Startup.cs**' or wherever you configure services, add the '**KafkaDataProducerService**':

```csharp
public void ConfigureServices(IServiceCollection services)
{
    // Other service configurations...

    services.Configure<KafkaProducerConfig>(Configuration.GetSection("KafkaProducerConfig"));
    services.AddSingleton<KafkaDataProducerService<string, string>>();
}
```

Usage Example
Inject and use the '**KafkaDataProducerService**' in your application:

```csharp
public class MyService
{
    private readonly KafkaDataProducerService<string, string> _producerService;

    public MyService(KafkaDataProducerService<string, string> producerService)
    {
        _producerService = producerService;
    }

    public async Task SendMessageAsync()
    {
        var message = new KafkaDataProducerMessage<string, string>
        {
            TopicName = "topic1",
            Key = "key1",
            Data = "Hello, Kafka!",
            OnProduceReportCallback = (msg, report) => 
            {
                // Handle produce report
                return Task.CompletedTask;
            },
            OnProduceExceptionCallback = (msg, ex) => 
            {
                // Handle exception
                return Task.CompletedTask;
            }
        };

        _producerService.EnqueueMessage(message);
        await _producerService.Start();
    }
}
```

#### Notes on KafkaDataProducerMessage some properties

##### OnProduceReportCallback

+ **Type**: '**Func<KafkaDataProducerMessage<TKey, TValue>, DeliveryResult<TKey, TValue>, Task>**'
+ **Required**: No (Optional)
+ **Description**: This callback function is invoked after a message has been successfully produced to Kafka. It provides an opportunity for additional processing or logging based on the successful delivery of the message. The callback receives two parameters: the message that was produced and the delivery report from Kafka, which contains details about the delivery.
+ **Behavior if Undefined**: If '**OnProduceReportCallback**' is not set, the Kafka producer will still send messages, but no additional action will be taken upon the successful delivery of messages. This means that you won't have custom logic executed for acknowledgment or logging tied to each message's delivery success.

##### OnProduceExceptionCallback

+ **Type**: '**Func<KafkaDataProducerMessage<TKey, TValue>, Exception, Task>**'
+ **Required**: No (Optional)
+ **Description**: This callback function is called when an exception occurs during the message production process. It allows for custom handling of exceptions, such as logging specific errors or performing recovery actions. The callback receives two parameters: the message that failed to be produced and the exception that was thrown. Implementing this callback can provide insights into any issues that occur during message production.
+ **Behavior if Undefined**: If '**OnProduceExceptionCallback**' is not defined, the Kafka producer will not perform any custom exception handling. This means that while exceptions will still be caught and logged at a general level by the producer service, you won't have specific custom logic executed for each exception. It is important to handle exceptions appropriately to ensure that your application can gracefully handle scenarios where message production fails.


### KafkaDataConsumerService
Dependency Injection
In your '**Startup.cs**' or wherever you configure services, add the '**KafkaDataConsumerService**':

```csharp
public void ConfigureServices(IServiceCollection services)
{
    // Other service configurations...

    services.Configure<KafkaConsumerConfig>(Configuration.GetSection("KafkaConsumerConfig"));
    services.AddSingleton(provider =>
    {
        var logger = provider.GetRequiredService<ILogger<KafkaDataConsumerService<string, string>>>();
        var config = provider.GetRequiredService<IConfiguration>();
        var kafkaOptions = provider.GetRequiredService<IOptions<KafkaConsumerConfig>>();
        return new KafkaDataConsumerService<string, string>(
            logger,
            config,
            kafkaOptions,
            ProcessMessageAsync
        );
    });
}

private static Task<bool> ProcessMessageAsync(Message<string, string> message)
{
    // Implement message processing logic
    return Task.FromResult(true); // Return true if processed successfully
}
```

Usage Example
Inject and use the '**KafkaDataConsumerService**' in your application:

```csharp
public class ConsumerService
{
    private readonly KafkaDataConsumerService<string, string> _consumerService;

    public ConsumerService(KafkaDataConsumerService<string, string> consumerService)
    {
        _consumerService = consumerService;
    }

    public async Task StartConsuming()
    {
        await _consumerService.Start();
        // Consume messages...
    }
}
```

### One more case for KafkaDataConsumerService Integration
#### Dependency Injection
'**KafkaDataConsumerService<TKey, TValue>**' can be injected into your application, allowing derived classes to provide custom message processing logic.

##### Configuration in Startup.cs
```c#
public void ConfigureServices(IServiceCollection services)
{
    // Other service configurations...

    services.Configure<KafkaConsumerConfig>(Configuration.GetSection("KafkaConsumerConfig"));
    services.AddSingleton<KafkaDataConsumerService<string, string>>();
}
```

##### Implementing a Derived Class
You can create a derived class that extends '**KafkaDataConsumerService<TKey, TValue>**' to provide custom message processing logic.

```c#
public class CustomKafkaConsumerService : KafkaDataConsumerService<string, string>
{
    public CustomKafkaConsumerService(
        ILogger<KafkaDataConsumerService<string, string>> logger,
        IConfiguration configuration,
        IOptions<KafkaConsumerConfig> kafkaConsumerOptions)
        : base(logger, configuration, kafkaConsumerOptions, ProcessMessageAsync)
    {
    }

    private static async Task<bool> ProcessMessageAsync(Message<string, string> message)
    {
        // Implement custom processing logic
        // Return true if processed successfully
        return true;
    }
}
```
##### Dependency Injection for Derived Class
Inject the derived consumer service in '**Startup.cs**':

```c#
public void ConfigureServices(IServiceCollection services)
{
    // Other service configurations...

    services.AddSingleton<CustomKafkaConsumerService>();
}
```
##### Usage Example
Use the derived consumer service in your application:
```c#
public class ConsumerHostedService : IHostedService
{
    private readonly CustomKafkaConsumerService _consumerService;

    public ConsumerHostedService(CustomKafkaConsumerService consumerService)
    {
        _consumerService = consumerService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _consumerService.Start();
        // Consume messages...
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _consumerService.Stop();
    }
}
```
##### Note on ProcessMessageAsync
+ **Required**: No (Optional)
+ **Description**: '**ProcessMessageAsync**' is a delegate that, if provided, is used to process each consumed message. It should be a function that takes a '**Message<TKey, TValue>**' and returns a '**Task**<**bool**>**', indicating whether the message was processed successfully.
+ **Behavior if Undefined**: If '**ProcessMessageAsync**' is not set in the derived class, the consumer service will enqueue the messages without processing them. In this scenario, the enqueued messages can be accessed and processed using the '**TryGetMessage**' method. This method attempts to dequeue a message from the internal queue, allowing for processing or handling elsewhere in the application. This method provides a non-blocking way to retrieve messages from the queue. It returns '**true**' if a message was successfully dequeued or '**false**' if the queue is empty.

##### Example Service for Consuming Messages
``` c#
public class MessageProcessingService
{
    private readonly KafkaDataConsumerService<string, string> _consumerService;
    private readonly CancellationTokenSource _cancellationTokenSource;

    public MessageProcessingService(KafkaDataConsumerService<string, string> consumerService)
    {
        _consumerService = consumerService;
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public void StartProcessing()
    {
        Task.Run(async () => await ProcessMessages());
    }

    private async Task ProcessMessages()
    {
        while (!_cancellationTokenSource.IsCancellationRequested)
        {
            if (_consumerService.TryGetMessage(out Message<string, string> message))
            {
                // Process the message
                ProcessMessage(message);
            }
            else
            {
                // No message in the queue, wait for a short duration before checking again
                await Task.Delay(1000); // Wait time can be adjusted as needed
            }
        }
    }

    private void ProcessMessage(Message<string, string> message)
    {
        // Implement the message processing logic here
        Console.WriteLine($"Message received: Key={message.Key}, Value={message.Value}");
    }

    public void StopProcessing()
    {
        _cancellationTokenSource.Cancel();
    }
}
```
##### Usage in the Application
Here's how you might use '**MessageProcessingService**' in your application:
```c#
public class MyApp
{
    public static void Main(string[] args)
    {
        // Assuming you have a configured KafkaDataConsumerService instance
        var consumerService = new KafkaDataConsumerService<string, string>(/* ...dependencies... */);

        var messageProcessingService = new MessageProcessingService(consumerService);
        messageProcessingService.StartProcessing();

        Console.WriteLine("Press any key to stop...");
        Console.ReadKey();

        messageProcessingService.StopProcessing();
    }
}
```
In this example, '**MessageProcessingService**' continuously checks for new messages using '**TryGetMessage**'. When a message is available, it's passed to '**ProcessMessage**' for further processing. The service runs in its own task, separate from the main thread, and can be stopped by calling '**StopProcessing**'.
