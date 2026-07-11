using System;
namespace am.kon.packages.services.kafka.Config
{
    /// <summary>
    /// Class to be used for storing Kafka producer configuration
    /// </summary>
    public class KafkaProducerConfig
    {
        /// <summary>
        /// Default section name for Kafka producer configuration
        /// </summary>
        public const string SectionDefaultName = "KafkaProducerConfig";


        /// <summary>
        /// Bootstrap servers to be used to connnect to Kafka
        /// </summary>
        public string BootstrapServers { get; set; }

        /// <summary>
        /// Maximum Kafka protocol request message size
        /// </summary>
        public int MessageMaxBytes { get; set; }

        /// <summary>
        /// Maximum Kafka protocol response message size.
        /// </summary>
        public int ReceiveMessageMaxBytes { get; set; }

        /// <summary>
        /// Local message timeout.
        /// </summary>
        public int MessageTimeoutMs { get; set; }

        /// <summary>
        /// The ack timeout of the producer request in milliseconds.
        /// </summary>
        public int RequestTimeoutMs { get; set; }

        /// <summary>
        /// Default timeout for network requests.
        /// </summary>
        public int SocketTimeoutMs { get; set; }

        /// <summary>
        /// Enable TCP keep-alives on broker sockets
        /// </summary>
        public bool SocketKeepaliveEnable { get; set; }

        /// <summary>
        /// Message compression type
        /// </summary>
        public string CompressionType { get; set; }

        /// <summary>
        /// Level of message compression
        /// </summary>
        public int CompressionLevel { get; set; }

        /// <summary>
        /// Number of acknowledgements required from Kafka before a produce request succeeds.
        /// Leave unset to preserve the underlying client default.
        /// </summary>
        public string Acks { get; set; }

        /// <summary>
        /// Enables the idempotent Kafka producer. Leave unset to preserve the underlying client default.
        /// </summary>
        public bool? EnableIdempotence { get; set; }

        /// <summary>
        /// Maximum number of in-flight requests per broker connection.
        /// When idempotence is enabled, this value must be no greater than five.
        /// Leave unset to preserve the underlying client default.
        /// </summary>
        public int? MaxInFlight { get; set; }

        /// <summary>
        /// Indicates whether to wait for the Topic Manager initialization process before sending messages.
        /// </summary>
        public bool AwaitForTopicManager { get; set; }
    }
}
