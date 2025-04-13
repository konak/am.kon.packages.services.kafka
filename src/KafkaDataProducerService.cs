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
    public class KafkaDataProducerService<TKey, TValue> : IDisposable
    {
        private readonly ILogger<KafkaDataProducerService<TKey, TValue>> _logger;
        private readonly IConfiguration _configuration;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        private readonly ConcurrentQueue<KafkaDataProducerMessage<TKey, TValue>> _messagesQueue;
        private readonly Timer _producerTimer;

        private readonly IProducer<TKey, TValue> _producer;
        private readonly ProducerConfig _producerConfig;
        private readonly KafkaProducerConfig _kafkaProducerConfig;

        private readonly KafkaTopicManagerService _kafkaTopicManagerService;

        private int _messagesQueueLength;
        private int _producingIsInProgress;

        private int _disposed;

        public int MessageQueueLength { get { return _messagesQueueLength; } }

        public KafkaDataProducerService(
            ILogger<KafkaDataProducerService<TKey, TValue>> logger,
            IConfiguration configuration,
            IOptions<KafkaProducerConfig> kafkaProducerOptions,
            KafkaTopicManagerService kafkaTopicManagerService
            )
        {
            _logger = logger;
            _configuration = configuration;

            _kafkaProducerConfig = kafkaProducerOptions.Value;

            _producerConfig = _kafkaProducerConfig.ToProducerConfig();

            _kafkaTopicManagerService = kafkaTopicManagerService;

            _messagesQueue = new ConcurrentQueue<KafkaDataProducerMessage<TKey, TValue>>();
            _messagesQueueLength = 0;

            _producer = new ProducerBuilder<TKey, TValue>(_producerConfig).Build();

            _producerTimer = new Timer(new TimerCallback(ProducerTimerHandler), null, Timeout.Infinite, Timeout.Infinite);
            _producingIsInProgress = 0;

            _disposed = 0;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        /// <summary>
        /// Starts the Kafka producer service, enabling the periodic sending of queued messages to the Kafka server.
        /// </summary>
        /// <returns>A task that represents the asynchronous start operation.</returns>
        public async Task Start()
        {
            if (_kafkaProducerConfig.AwaitForTopicManager)
                await _kafkaTopicManagerService.WaitForTopicsCreation();

            _producerTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// Stop the Kafka producer service
        /// </summary>
        /// <returns>A task that represents the asynchronous stop operation.</returns>
        public Task Stop()
        {
            _cancellationTokenSource.Cancel();
            _producerTimer.Change(Timeout.Infinite, Timeout.Infinite);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Enqueue message to send to Kafka
        /// </summary>
        /// <param name="message">The Kafka message to be enqueued for delivery.</param>
        public void EnqueueMessage(KafkaDataProducerMessage<TKey, TValue> message)
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
        private async Task ProduceQueueToKafka()
        {
            try
            {
                while (!_cancellationToken.IsCancellationRequested && _messagesQueue.TryDequeue(out KafkaDataProducerMessage<TKey, TValue> message))
                {
                    Interlocked.Decrement(ref _messagesQueueLength);

                    try
                    {
                        DeliveryResult<TKey, TValue> deliveryReport = await _producer.ProduceAsync(message.TopicName, message.ToMessage(), _cancellationToken);

                        if (!_cancellationToken.IsCancellationRequested && message.OnProduceReportCallback != null)
                            await message.OnProduceReportCallback(message, deliveryReport);
                    }
                    catch (KafkaException ex)
                    {
                        if (message.OnProduceExceptionCallback != null)
                        {
                            try
                            {
                                await message.OnProduceExceptionCallback(message, ex);
                            }
                            catch (Exception exx)
                            {
                                _logger.LogError(exx, "Unhandled exception on callback of produce exception handler.");
                            }
                        }
                        else
                        {
                            _logger.LogError(ex, $"Kafka exception for message with key {message.Key} on topic {message.TopicName}.");
                        }
                    }
                    catch (Exception ex)
                    {
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

        /// <summary>
        /// Method to dispose all disposable resources
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if(!disposing)
                return;

            int originalValue = Interlocked.CompareExchange(ref _disposed, 1, 0);

            if (originalValue != 0)
                return;

            _producer?.Dispose();
            _cancellationTokenSource?.Dispose();
            _producerTimer?.Dispose();
        }

        /// <summary>
        /// Dispose method implementation of IDisposable interface
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
    }
}

