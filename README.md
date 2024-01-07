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

### KafkaProducerConfig and KafkaConsumerConfig

These sections define the settings for your Kafka producer and consumer, including bootstrap servers, timeouts, compression settings, and consumer group configuration.

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


# am.kon.packages.services.kafka