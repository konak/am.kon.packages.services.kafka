using am.kon.packages.services.kafka.Config;

namespace am.kon.packages.services.kafka.tests;

public sealed class KafkaTopicSpecificationResolverTests
{
    [Fact]
    public void Resolve_WhenLegacyTopicsAreConfigured_UsesDefaultsAndDefaultConfigs()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = [" orders.events.v1 "];
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["min.insync.replicas"] = "2",
            ["unclean.leader.election.enable"] = "false"
        };

        var result = KafkaTopicSpecificationResolver.Resolve(config);

        var topic = Assert.Single(result);
        Assert.Equal("orders.events.v1", topic.Name);
        Assert.Equal(3, topic.NumPartitions);
        Assert.Equal((short)3, topic.ReplicationFactor);
        Assert.Equal("2", topic.Configs!["min.insync.replicas"]);
        Assert.Equal("false", topic.Configs["unclean.leader.election.enable"]);
    }

    [Fact]
    public void Resolve_WhenLegacyTopicsContainDuplicates_PreservesLegacyDeduplication()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1", " ORDERS.EVENTS.V1 ", "", "  "];

        var result = KafkaTopicSpecificationResolver.Resolve(config);

        Assert.Single(result);
        Assert.Equal("orders.events.v1", result[0].Name);
    }

    [Fact]
    public void Resolve_WhenDetailedTopicIsConfigured_AppliesOverridesAndMergesConfigs()
    {
        var config = CreateConfig();
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["min.insync.replicas"] = "2",
            ["cleanup.policy"] = "delete"
        };
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig
            {
                Name = "orders.compacted.v1",
                NumPartitions = 6,
                ReplicationFactor = 3,
                Configs = new Dictionary<string, string>
                {
                    ["cleanup.policy"] = "compact"
                }
            }
        ];

        var result = KafkaTopicSpecificationResolver.Resolve(config);

        var topic = Assert.Single(result);
        Assert.Equal(6, topic.NumPartitions);
        Assert.Equal((short)3, topic.ReplicationFactor);
        Assert.Equal("2", topic.Configs!["min.insync.replicas"]);
        Assert.Equal("compact", topic.Configs["cleanup.policy"]);
    }

    [Fact]
    public void Resolve_WhenDetailedValuesAreOmitted_FallsBackToSectionDefaults()
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig { Name = "orders.events.v1" }
        ];

        var topic = Assert.Single(KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Equal(3, topic.NumPartitions);
        Assert.Equal((short)3, topic.ReplicationFactor);
    }

    [Fact]
    public void Resolve_WhenDetailedTopicMatchesLegacyTopic_UsesDetailedDefinitionOnce()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig
            {
                Name = "ORDERS.EVENTS.V1",
                NumPartitions = 6,
                ReplicationFactor = 3
            }
        ];

        var topic = Assert.Single(KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Equal("ORDERS.EVENTS.V1", topic.Name);
        Assert.Equal(6, topic.NumPartitions);
    }

    [Fact]
    public void Resolve_WhenNoTopicsAreConfigured_PreservesEmptyLegacyBehavior()
    {
        var config = new KafkaTopicManagerConfig();

        var result = KafkaTopicSpecificationResolver.Resolve(config);

        Assert.Empty(result);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Resolve_WhenPartitionCountIsNotPositive_Throws(int partitionCount)
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig
            {
                Name = "orders.events.v1",
                NumPartitions = partitionCount,
                ReplicationFactor = 3
            }
        ];

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("positive partition count", exception.Message);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)-1)]
    public void Resolve_WhenReplicationFactorIsNotPositive_Throws(short replicationFactor)
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig
            {
                Name = "orders.events.v1",
                NumPartitions = 3,
                ReplicationFactor = replicationFactor
            }
        ];

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("positive replication factor", exception.Message);
    }

    [Fact]
    public void Resolve_WhenLegacyTopicUsesInvalidDefaults_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.NumPartitionsDefault = 0;

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("positive partition count", exception.Message);
    }

    [Fact]
    public void Resolve_WhenMinInSyncReplicasExceedsReplicationFactor_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["min.insync.replicas"] = "4"
        };

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("exceeds replication factor 3", exception.Message);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("not-an-integer")]
    public void Resolve_WhenMinInSyncReplicasIsInvalid_Throws(string value)
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["min.insync.replicas"] = value
        };

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("invalid min.insync.replicas", exception.Message);
    }

    [Fact]
    public void Resolve_WhenDetailedSpecificationIsDuplicated_ThrowsDuplicateError()
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig { Name = "orders.events.v1" },
            new KafkaTopicCreationConfig { Name = "ORDERS.EVENTS.V1" }
        ];

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("Duplicate detailed Kafka topic specification", exception.Message);
    }

    [Fact]
    public void Resolve_WhenDetailedSpecificationsConflict_ThrowsConflictError()
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig { Name = "orders.events.v1", NumPartitions = 3 },
            new KafkaTopicCreationConfig { Name = "orders.events.v1", NumPartitions = 6 }
        ];

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("Conflicting detailed Kafka topic specification", exception.Message);
    }

    [Fact]
    public void Resolve_WhenDetailedTopicNameIsEmpty_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig { Name = " " }
        ];

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("non-empty topic name", exception.Message);
    }

    [Fact]
    public void Resolve_WhenConfigKeyIsEmpty_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.TopicConfigsDefault = new Dictionary<string, string> { [" "] = "value" };

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("empty Kafka config key", exception.Message);
    }

    [Fact]
    public void Resolve_WhenConfigKeysConflictByCase_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "delete",
            ["CLEANUP.POLICY"] = "compact"
        };

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("conflicting values for Kafka config", exception.Message);
    }

    [Fact]
    public void Resolve_WhenConfigValueIsNull_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopics = ["orders.events.v1"];
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["cleanup.policy"] = null!
        };

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("has a null value", exception.Message);
    }

    [Fact]
    public void Resolve_WhenTopicOverridesDefaultMinInSyncReplicas_ValidatesResolvedValue()
    {
        var config = CreateConfig();
        config.TopicConfigsDefault = new Dictionary<string, string>
        {
            ["min.insync.replicas"] = "4"
        };
        config.EnsureExistTopicSpecifications =
        [
            new KafkaTopicCreationConfig
            {
                Name = "orders.events.v1",
                Configs = new Dictionary<string, string>
                {
                    ["min.insync.replicas"] = "2"
                }
            }
        ];

        var topic = Assert.Single(KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Equal("2", topic.Configs!["min.insync.replicas"]);
    }

    [Fact]
    public void Resolve_WhenDetailedSpecificationEntryIsNull_Throws()
    {
        var config = CreateConfig();
        config.EnsureExistTopicSpecifications = [null!];

        var exception = Assert.Throws<ArgumentException>(
            () => KafkaTopicSpecificationResolver.Resolve(config));

        Assert.Contains("cannot contain null entries", exception.Message);
    }

    private static KafkaTopicManagerConfig CreateConfig()
    {
        return new KafkaTopicManagerConfig
        {
            NumPartitionsDefault = 3,
            ReplicationFactorDefault = 3
        };
    }
}
