using System;
using Confluent.Kafka;
using System.Threading.Tasks;

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

        /// <summary>
        /// A callback function that is invoked after a message has been successfully produced to Kafka.
        /// This callback provides an opportunity for additional processing or logging based on the
        /// successful delivery of the message. It receives two parameters: the message that was produced
        /// and the delivery report from Kafka, which contains details about the delivery.
        /// </summary>
        public Func<KafkaDataProducerMessage<TKey, TValue>, DeliveryResult<TKey, TValue>, Task> OnProduceReportCallback { get; set; }

        /// <summary>
        /// A callback function that is called when an exception occurs during the message production process.
        /// This callback allows for custom handling of exceptions, such as logging specific errors or performing
        /// recovery actions. It receives two parameters: the message that failed to be produced and the exception
        /// that was thrown. Implementors of this callback should handle exceptions within the callback to avoid
        /// unhandled exceptions.
        /// </summary>
        public Func<KafkaDataProducerMessage<TKey, TValue>, Exception, Task> OnProduceExceptionCallback { get; set; }
    }
}

