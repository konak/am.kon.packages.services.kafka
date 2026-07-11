using am.kon.packages.services.kafka.Config;
using am.kon.packages.services.kafka.Extensions;
using Confluent.Kafka;

namespace am.kon.packages.services.kafka.tests;

public sealed class KafkaConsumerConfigExtensionsTests
{
    [Fact]
    public void ToConsumerConfig_WhenAutoOffsetResetIsOmitted_PreservesClientDefault()
    {
        var result = CreateConfig().ToConsumerConfig();

        Assert.Null(result.AutoOffsetReset);
    }

    [Theory]
    [InlineData(AutoOffsetReset.Earliest)]
    [InlineData(AutoOffsetReset.Latest)]
    [InlineData(AutoOffsetReset.Error)]
    public void ToConsumerConfig_WhenAutoOffsetResetIsConfigured_MapsIt(AutoOffsetReset value)
    {
        var config = CreateConfig();
        config.AutoOffsetReset = value;

        var result = config.ToConsumerConfig();

        Assert.Equal(value, result.AutoOffsetReset);
    }

    private static KafkaConsumerConfig CreateConfig()
    {
        return new KafkaConsumerConfig
        {
            BootstrapServers = "broker-1:9092,broker-2:9092,broker-3:9092",
            MessageMaxBytes = 1_000_000,
            SocketTimeoutMs = 60_000,
            GroupId = "orders-history-writer",
            AutoCommit = false,
            MakeGroupUnique = false,
            Topic = "orders.history.v1"
        };
    }
}
