using System;
namespace am.kon.packages.services.kafka.Config
{
    /// <summary>
    /// Class to be used for storing Kafka consumer configuration
    /// </summary>
    public class KafkaConsumerConfig
    {
        /// <summary>
        /// Default section name for Kafka consumer configuration
        /// </summary>
        public const string SectionDefaultName = "KafkaConsumerConfig";

        /// <summary>
        /// Bootstrap servers to be used to connnect to Kafka
        /// </summary>
        public string BootstrapServers { get; set; }

        /// <summary>
        /// Maximum Kafka protocol request message size
        /// </summary>
        public int MessageMaxBytes { get; set; }

        /// <summary>
        /// Default timeout for network requests.
        /// </summary>
        public int SocketTimeoutMs { get; set; }

        /// <summary>
        /// Id of the groupp consumer to be part of
        /// </summary>
        public string GroupId { get; set; }

        /// <summary>
        /// Try to make group ID uninque by adding time of connection
        /// </summary>
        public bool MakeGroupUnique { get; set; }

        /// <summary>
        /// Autocommit acnowledgement of Kafka message consuming
        /// </summary>
        public bool AutoCommit { get; set; }

        /// <summary>
        /// Name of topics to subscribe to
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// Indicates whether to wait for the Topic Manager to initialize before consuming messages
        /// </summary>
        public bool AwaitForTopicManager { get; set; }
    }
}
