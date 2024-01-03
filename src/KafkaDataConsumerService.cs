using System;
using am.kon.packages.services.kafka.Extensions;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using am.kon.packages.services.kafka.Config;

namespace am.kon.packages.services.kafka
{
    /// <summary>
    /// Service to be used for consuming data from Kafka server
    /// </summary>
    public class KafkaDataConsumerService<TKey, TValue>
    {
        private readonly ILogger<KafkaDataConsumerService<TKey, TValue>> _logger;
        private readonly IConfiguration _configuration;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        private readonly ConcurrentQueue<Message<TKey, TValue>> _messagesQueue;
        private volatile int _messagesQueueLength;

        private readonly Timer _consumerTimer;
        private volatile int _consumingIsInProgress;

        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ConsumerConfig _consumerConfig;

        private readonly KafkaConsumerConfig _kafkaCoonsumerConfig;

        private bool _disposed = false;

        protected readonly Func<Message<TKey, TValue>, Task<bool>> _processMessageAsync;

        public KafkaDataConsumerService(
            ILogger<KafkaDataConsumerService<TKey, TValue>> logger,
            IConfiguration configuration,

            IOptions<KafkaConsumerConfig> kafkaConsumerOptions,
            Func<Message<TKey, TValue>, Task<bool>> processMessageAsync = null
            )
        {
            _logger = logger;
            _configuration = configuration;

            _messagesQueue = new ConcurrentQueue<Message<TKey, TValue>>();
            _messagesQueueLength = 0;

            _kafkaCoonsumerConfig = kafkaConsumerOptions.Value;
            _consumerConfig = _kafkaCoonsumerConfig.ToConsumerConfig();

            if (_kafkaCoonsumerConfig.MakeGroupUnique)
                _consumerConfig.GroupId += $"_{DateTime.Now}";

            _consumer = new ConsumerBuilder<TKey, TValue>(_consumerConfig).Build();

            _processMessageAsync = processMessageAsync;

            _consumerTimer = new Timer(new TimerCallback(ConsumerTimerHandler), null, Timeout.Infinite, Timeout.Infinite);
            _consumingIsInProgress = 0;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        /// <summary>
        /// Start service
        /// </summary>
        /// <returns></returns>
        public Task Start()
        {
            foreach (string topicName in _kafkaCoonsumerConfig.Topics)
            {
                _consumer.Subscribe(topicName);
            }

            _consumerTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
            return Task.CompletedTask;
        }

        /// <summary>
        /// Stop Service
        /// </summary>
        /// <returns></returns>
        public Task Stop()
        {
            _cancellationTokenSource.Cancel();
            _consumerTimer.Change(Timeout.Infinite, Timeout.Infinite);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Handler function for consuming initiation timer
        /// </summary>
        /// <param name="status"></param>
        private void ConsumerTimerHandler(object status)
        {
            int originalValue = Interlocked.CompareExchange(ref _consumingIsInProgress, 1, 0);

            if (originalValue == 1 || _cancellationToken.IsCancellationRequested)
                return;

            _ = StartConsuming();
        }

        /// <summary>
        /// Consuming async thread
        /// </summary>
        /// <returns></returns>
        private async Task StartConsuming()
        {
            try
            {
                while (!_cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<TKey, TValue> consumeResult = await Task.Run(() => _consumer.Consume(_cancellationToken));

                        if (consumeResult.IsPartitionEOF)
                            return;

                        bool commitConsume = !_kafkaCoonsumerConfig.AutoCommit;

                        if(_processMessageAsync == null)
                        {
                            _messagesQueue.Enqueue(consumeResult.Message);
                            Interlocked.Increment(ref _messagesQueueLength);
                        }
                        else
                        {
                            try
                            {
                                if (!await _processMessageAsync(consumeResult.Message))
                                {
                                    commitConsume = false;
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError("Unhandled exception in Kafka message processing delegate.", ex);
                                commitConsume = false;
                            }
                        }

                        if (commitConsume)
                            _consumer.Commit(consumeResult);

                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Consume error. ClientID: {_consumer.Name}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled exception during consuming data from Kafka server.");
            }
            finally
            {
                Interlocked.Exchange(ref _consumingIsInProgress, 0);
            }
        }

        /// <summary>
        /// Tries to retrieve a consumed message from the queue.
        /// </summary>
        /// <param name="message">When this method returns, contains the consumed message if the method succeeded, 
        /// or the default value for the type of the message parameter if the method failed.</param>
        /// <returns>Returns <c>true</c> if a message was successfully dequeued; otherwise, <c>false</c>.</returns>
        /// <remarks>
        /// This method attempts to dequeue a message from the internal message queue. It is a non-blocking method
        /// and will return immediately. If the queue is empty, the method will return <c>false</c>, and the output
        /// parameter 'message' will be set to its default value.
        /// </remarks>
        public bool TryGetMessage(out Message<TKey, TValue> message)
        {
            if (_messagesQueue.TryDequeue(out message))
            {
                Interlocked.Decrement(ref _messagesQueueLength);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Method to dispose all disposable resources
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _consumer?.Dispose();
                _cancellationTokenSource?.Dispose();
                _consumerTimer?.Dispose();
            }

            _disposed = true;
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

