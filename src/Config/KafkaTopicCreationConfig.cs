using System.Collections.Generic;

namespace am.kon.packages.services.kafka.Config
{
    /// <summary>
    /// Describes how a missing Kafka topic should be created.
    /// Existing topics are never altered by the topic manager.
    /// </summary>
    public class KafkaTopicCreationConfig
    {
        public string Name { get; set; }

        public int? NumPartitions { get; set; }

        public short? ReplicationFactor { get; set; }

        public Dictionary<string, string> Configs { get; set; }
    }
}
