[![.net build main and dev](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet.yml/badge.svg)](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet.yml)
[![.net package publish to nuget](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet-beta.yml/badge.svg)](https://github.com/konak/am.kon.packages.services.kafka/actions/workflows/dotnet-beta.yml)

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
    "CompressionLevel": 5
  },
  "KafkaConsumerConfig": {
    "BootstrapServers": "localhost:9092",
    "MessageMaxBytes": 1000000,
    "SocketTimeoutMs": 60000,
    "GroupId": "my-consumer-group",
    "MakeGroupUnique": false,
    "AutoCommit": true,
    "Topics": ["topic1", "topic2"]
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

### KafkaConsumerConfig Properties

+ **BootstrapServers**: Similar to the producer, it's a list of Kafka broker addresses.
+ **MessageMaxBytes**: Controls the maximum size of a fetch message. Helps to control memory usage of the consumer.
+ **SocketTimeoutMs**: The timeout for network requests.
+ **GroupId**: The name of the consumer group this consumer belongs to. Consumer groups allow a group of consumers to cooperate in consuming the messages.
+ **MakeGroupUnique**: When set to true, appends a unique identifier (timestamp) to the GroupId, creating a unique consumer group on every run.
+ **AutoCommit**: If set to true, the consumer's offset will be periodically committed in the background.
+ **Topics**: An array of topics this consumer should subscribe to.

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

# am.kon.packages.services.kafka