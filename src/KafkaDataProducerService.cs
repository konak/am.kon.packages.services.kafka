using System;
using am.kon.packages.services.kafka.Config;
using am.kon.packages.services.kafka.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using am.kon.packages.services.kafka.Extensions;

namespace am.kon.packages.services.kafka
{
    /// <summary>
    /// Service to be used for producing data to Kafka server
    /// </summary>
    public class KafkaDataProducerService<TKey, TValue>
    {
        private readonly ILogger<KafkaDataProducerService<TKey, TValue>> _logger;
        private readonly IConfiguration _configuration;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        private readonly ConcurrentQueue<KafkaDataProducerMessage<TKey, TValue>> _messagesQueue;
        private readonly Timer _producerTimer;

        private readonly IProducer<TKey, TValue> _producer;
        private readonly ProducerConfig _producerConfig;
        private readonly KafkaProducerConfig _kafkaProducerCoonfig;

        private volatile int _messagesQueueLength;
        private volatile int _producingIsInProgress;


        public int MessageQueueLength { get { return _messagesQueueLength; } }

        public KafkaDataProducerService(
            ILogger<KafkaDataProducerService<TKey, TValue>> logger,
            IConfiguration configuration,
            IOptions<KafkaProducerConfig> kafkaProducerOptions
            )
        {
            _logger = logger;
            _configuration = configuration;

            _kafkaProducerCoonfig = kafkaProducerOptions.Value;

            _producerConfig = _kafkaProducerCoonfig.ToProducerConfig();

            _messagesQueue = new ConcurrentQueue<KafkaDataProducerMessage<TKey, TValue>>();
            _messagesQueueLength = 0;

            _producer = new ProducerBuilder<TKey, TValue>(_producerConfig).Build();

            _producerTimer = new Timer(new TimerCallback(ProducerTimerHandler), null, Timeout.Infinite, Timeout.Infinite);
            _producingIsInProgress = 0;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        /// <summary>
        /// Start service
        /// </summary>
        /// <returns></returns>
        public Task Start()
        {
            _producerTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
            return Task.CompletedTask;
        }

        /// <summary>
        /// Stop Service
        /// </summary>
        /// <returns></returns>
        public Task Stop()
        {
            _cancellationTokenSource.Cancel();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Enqueue message to send to Kafka
        /// </summary>
        /// <param name="message">Message to be send to Kafaka server</param>
        public void EnqueMessage(KafkaDataProducerMessage<TKey, TValue> message)
        {
            _messagesQueue.Enqueue(message);
            Interlocked.Increment(ref _messagesQueueLength);
        }

        /// <summary>
        /// Timer function handler to initiate background thread of data producing to Kafka server
        /// </summary>
        /// <param name="state"></param>
        private void ProducerTimerHandler(object state)
        {
            if (_messagesQueueLength == 0 || _cancellationToken.IsCancellationRequested) return;

            int originalValue = Interlocked.CompareExchange(ref _producingIsInProgress, 1, 0);

            if (originalValue == 1) return;

            _ = ProduceQueueToKafka();
        }

        /// <summary>
        /// Background async task producing data to Kafka server
        /// </summary>
        /// <returns></returns>
        private async Task ProduceQueueToKafka(Func<KafkaDataProducerMessage<TKey, TValue>, DeliveryResult<TKey, TValue>, Task> onProduceReport = null, Func<KafkaDataProducerMessage<TKey, TValue>, Exception, Task> onProduceException = null)
        {
            try
            {
                while (!_cancellationToken.IsCancellationRequested && _messagesQueue.TryDequeue(out KafkaDataProducerMessage<TKey, TValue> message))
                {
                    Interlocked.Decrement(ref _messagesQueueLength);

                    try
                    {
                        DeliveryResult<TKey, TValue> deliveryReport = await _producer.ProduceAsync(message.TopicName, message.ToMessage(), _cancellationToken);

                        if (!_cancellationToken.IsCancellationRequested && onProduceReport != null)
                            await onProduceReport(message, deliveryReport);
                    }
                    catch (Exception ex)
                    {
                        if (onProduceException != null)
                        {
                            try
                            {
                                await onProduceException(message, ex);
                            }
                            catch (Exception exx)
                            {
                                _logger.LogError(exx, "Unhandled exception on callback of produce exception handler.");
                            }
                        }
                        else
                            _logger.LogError(ex, "Unhandled exception on message delivery to kafka.");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled exception in produce queue to kafka.");
            }
            finally
            {
                Interlocked.Exchange(ref _producingIsInProgress, 0);
            }
        }
    }
}

