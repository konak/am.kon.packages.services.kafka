using System;
using am.kon.packages.services.kafka.Config;
using Confluent.Kafka;

namespace am.kon.packages.services.kafka.Extensions
{
    /// <summary>
    /// Class to implement extensions for <see cref="KafkaProducerConfig"/>
    /// </summary>
    public static class KafkaProducerConfigExtensions
    {
        /// <summary>
        /// Convert instance of <see cref="KafkaProducerConfig"/> into <see cref="ProducerConfig"/> one
        /// </summary>
        /// <param name="kafkaProducerConfig">Instance of the <see cref="KafkaProducerConfig"/> class.</param>
        /// <returns>Instance of the <see cref="ProducerConfig"/> class.</returns>
        public static ProducerConfig ToProducerConfig(this KafkaProducerConfig kafkaProducerConfig)
        {
            if (kafkaProducerConfig == null)
                throw new ArgumentNullException(nameof(kafkaProducerConfig));

            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = kafkaProducerConfig.BootstrapServers,
                MessageMaxBytes = kafkaProducerConfig.MessageMaxBytes,
                ReceiveMessageMaxBytes = kafkaProducerConfig.ReceiveMessageMaxBytes,
                MessageTimeoutMs = kafkaProducerConfig.MessageTimeoutMs,
                RequestTimeoutMs = kafkaProducerConfig.RequestTimeoutMs,
                SocketTimeoutMs = kafkaProducerConfig.SocketTimeoutMs,
                SocketKeepaliveEnable = kafkaProducerConfig.SocketKeepaliveEnable,
                CompressionType = (CompressionType)Enum.Parse(typeof(CompressionType), kafkaProducerConfig.CompressionType, true),
                CompressionLevel = kafkaProducerConfig.CompressionLevel
            };

            if (!string.IsNullOrWhiteSpace(kafkaProducerConfig.Acks))
                producerConfig.Acks = ParseAcks(kafkaProducerConfig.Acks);

            if (kafkaProducerConfig.EnableIdempotence.HasValue)
                producerConfig.EnableIdempotence = kafkaProducerConfig.EnableIdempotence.Value;

            if (kafkaProducerConfig.MaxInFlight.HasValue)
                producerConfig.MaxInFlight = kafkaProducerConfig.MaxInFlight.Value;

            return producerConfig;
        }

        private static Acks ParseAcks(string value)
        {
            if (Enum.TryParse(value, true, out Acks parsed)
                && (parsed == Acks.None || parsed == Acks.Leader || parsed == Acks.All))
                return parsed;

            throw new ArgumentException(
                $"Unsupported Kafka producer acknowledgement value '{value}'. Use 'none', 'leader', 'all', '0', '1', or '-1'.",
                nameof(value));
        }
    }
}
