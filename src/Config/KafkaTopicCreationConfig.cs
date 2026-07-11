using System.Collections.Generic;

namespace am.kon.packages.services.kafka.Config
{
    /// <summary>
    /// Describes how a missing Kafka topic should be created and which supported
    /// configuration values may be reconciled when existing-topic reconciliation is enabled.
    /// </summary>
    public class KafkaTopicCreationConfig
    {
        public string Name { get; set; }

        public int? NumPartitions { get; set; }

        public short? ReplicationFactor { get; set; }

        public Dictionary<string, string> Configs { get; set; }
    }
}
