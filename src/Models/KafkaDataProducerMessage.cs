using System;
namespace am.kon.packages.services.kafka.Models
{
    /// <summary>
    /// Class to be used for messages to produce to kafka
    /// </summary>
    public class KafkaDataProducerMessage<TKey, TValue>
    {
        /// <summary>
        /// Name of the topic to publish messages to
        /// </summary>
        public string TopicName { get; set; }

        /// <summary>
        /// Key of the message to be published
        /// </summary>
        public TKey Key { get; set; }

        /// <summary>
        /// Data of the message to be published
        /// </summary>
        public TValue Data { get; set; }
    }
}

