using System.Collections.Generic;

namespace am.kon.packages.services.kafka.Config
{
    public class KafkaTopicManagerConfig
    {
        public const string SectionDefaultName = "KafkaTopicManager";

        public string BootstrapServers { get; set; }

        public string[] EnsureExistTopics { get; set; }

        public int NumPartitionsDefault { get; set; }

        public short ReplicationFactorDefault { get; set; }

        /// <summary>
        /// Optional topic-level configuration applied to every topic created by this manager.
        /// Per-topic values in <see cref="EnsureExistTopicSpecifications"/> take precedence.
        /// </summary>
        public Dictionary<string, string> TopicConfigsDefault { get; set; }

        /// <summary>
        /// Optional detailed topic definitions. These may be used alongside
        /// <see cref="EnsureExistTopics"/> while services migrate from the legacy string list.
        /// </summary>
        public KafkaTopicCreationConfig[] EnsureExistTopicSpecifications { get; set; }
    }
}
