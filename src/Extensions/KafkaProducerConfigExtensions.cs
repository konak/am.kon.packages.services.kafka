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
            return new ProducerConfig()
            {
                BootstrapServers = kafkaProducerConfig.BootstrapServers,
                MessageMaxBytes = kafkaProducerConfig.MessageMaxBytes,
                ReceiveMessageMaxBytes = kafkaProducerConfig.ReceiveMessageMaxBytes,
                MessageTimeoutMs = kafkaProducerConfig.MessageTimeoutMs,
                RequestTimeoutMs = kafkaProducerConfig.RequestTimeoutMs,
                SocketTimeoutMs = kafkaProducerConfig.SocketTimeoutMs,
                SocketKeepaliveEnable = kafkaProducerConfig.SocketKeepaliveEnable,
                CompressionType = (CompressionType)Enum.Parse(typeof(CompressionType), kafkaProducerConfig.CompressionType, true),
                CompressionLevel = kafkaProducerConfig.CompressionLevel//,
            };
        }
    }
}

