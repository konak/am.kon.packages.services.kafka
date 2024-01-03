using System;
using am.kon.packages.services.kafka.Config;
using Confluent.Kafka;

namespace am.kon.packages.services.kafka.Extensions
{
    /// <summary>
    /// Class to implement extensions for <see cref="KafkaConsumerConfig"/>
    /// </summary>
    public static class KafkaConsumerConfigExtensions
    {
        /// <summary>
        /// Convert instance of <see cref="KafkaConsumerConfig"/> into <see cref="ConsumerConfig"/> one
        /// </summary>
        /// <param name="kafkaConsumerConfig">Instance of the <see cref="KafkaConsumerConfig"/> class.</param>
        /// <returns>Instance of the <see cref="ConsumerConfig"/> class.</returns>
        public static ConsumerConfig ToConsumerConfig(this KafkaConsumerConfig kafkaConsumerConfig)
        {
            ConsumerConfig res = new ConsumerConfig()
            {
                BootstrapServers = kafkaConsumerConfig.BootstrapServers,
                MessageMaxBytes = kafkaConsumerConfig.MessageMaxBytes,
                SocketTimeoutMs = kafkaConsumerConfig.SocketTimeoutMs,
                GroupId = kafkaConsumerConfig.GroupId,
                EnableAutoCommit = kafkaConsumerConfig.AutoCommit
            };

            return res;
        }
    }
}

