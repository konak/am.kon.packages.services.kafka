using am.kon.packages.services.kafka.Config;
using am.kon.packages.services.kafka.Extensions;
using Confluent.Kafka;

namespace am.kon.packages.services.kafka.tests;

public sealed class KafkaProducerConfigExtensionsTests
{
    [Fact]
    public void ToProducerConfig_WhenDurabilityOptionsAreOmitted_PreservesClientDefaults()
    {
        var result = CreateConfig().ToProducerConfig();

        Assert.Null(result.Acks);
        Assert.Null(result.EnableIdempotence);
        Assert.Null(result.MaxInFlight);
    }

    [Fact]
    public void ToProducerConfig_WhenDurabilityOptionsAreConfigured_MapsThem()
    {
        var config = CreateConfig();
        config.Acks = "all";
        config.EnableIdempotence = true;
        config.MaxInFlight = 5;

        var result = config.ToProducerConfig();

        Assert.Equal(Acks.All, result.Acks);
        Assert.True(result.EnableIdempotence);
        Assert.Equal(5, result.MaxInFlight);
    }

    [Theory]
    [InlineData("0", Acks.None)]
    [InlineData("1", Acks.Leader)]
    [InlineData("-1", Acks.All)]
    [InlineData("none", Acks.None)]
    [InlineData("leader", Acks.Leader)]
    [InlineData("all", Acks.All)]
    public void ToProducerConfig_WhenAcksUsesSupportedValue_MapsIt(string value, Acks expected)
    {
        var config = CreateConfig();
        config.Acks = value;

        var result = config.ToProducerConfig();

        Assert.Equal(expected, result.Acks);
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("2")]
    public void ToProducerConfig_WhenAcksIsInvalid_ThrowsClearException(string value)
    {
        var config = CreateConfig();
        config.Acks = value;

        var exception = Assert.Throws<ArgumentException>(() => config.ToProducerConfig());

        Assert.Contains("Unsupported Kafka producer acknowledgement value", exception.Message);
    }

    private static KafkaProducerConfig CreateConfig()
    {
        return new KafkaProducerConfig
        {
            BootstrapServers = "broker-1:9092,broker-2:9092,broker-3:9092",
            MessageMaxBytes = 1_000_000,
            ReceiveMessageMaxBytes = 1_000_000,
            MessageTimeoutMs = 30_000,
            RequestTimeoutMs = 5_000,
            SocketTimeoutMs = 60_000,
            SocketKeepaliveEnable = true,
            CompressionType = "gzip",
            CompressionLevel = 5
        };
    }
}
