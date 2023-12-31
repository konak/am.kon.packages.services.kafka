using System;
using am.kon.packages.services.kafka.Models;
using Confluent.Kafka;

namespace am.kon.packages.services.kafka.Extensions
{
    /// <summary>
    /// Extension methods for <see cref="KafkaDataProducerMessage<TKey, TValue>"/> objects
    /// </summary>
    public static class KafkaDataProducerMessageExtensions
    {
        /// <summary>
        /// Extension method to converts instance of <see cref="KafkaDataProducerMessage<TKey, TValue>"/> into instance of <seealso cref="Message<TKey, TValue>"/> object.
        /// </summary>
        /// <typeparam name="TKey">Type of the key of the message</typeparam>
        /// <typeparam name="TValue">Type of the value of the message</typeparam>
        /// <param name="producerMessage">Instance of the <see cref="KafkaDataProducerMessage<TKey, TValue>"/> object to be converted.</param>
        /// <returns>Converted instance of <see cref="Message<TKey, TValue>"/> object</returns>
        public static Message<TKey, TValue> ToMessage<TKey, TValue>(this KafkaDataProducerMessage<TKey, TValue> producerMessage)
        {
            Message<TKey, TValue> res = new Message<TKey, TValue>()
            {
                Key = producerMessage.Key,
                Value = producerMessage.Data
            };

            return res;
        }
    }
}

