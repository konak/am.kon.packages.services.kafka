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
        protected readonly IConfiguration Configuration;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        private readonly ConcurrentQueue<Message<TKey, TValue>> _messagesQueue;
        private int _messagesQueueLength;

        private readonly Timer _consumerTimer;
        private int _consumingIsInProgress;

        private readonly IConsumer<TKey, TValue> _consumer;
        protected readonly ConsumerConfig ConsumerConfig;

        private readonly KafkaConsumerConfig _kafkaConsumerConfig;

        private int _disposed;

        protected readonly Func<Message<TKey, TValue>, Task<bool>> ProcessMessageAsync;

        public KafkaDataConsumerService(
            ILogger<KafkaDataConsumerService<TKey, TValue>> logger,
            IConfiguration configuration,

            IOptions<KafkaConsumerConfig> kafkaConsumerOptions,
            Func<Message<TKey, TValue>, Task<bool>> processMessageAsync = null
            )
        {
            _logger = logger;
            Configuration = configuration;

            _messagesQueue = new ConcurrentQueue<Message<TKey, TValue>>();
            _messagesQueueLength = 0;

            _kafkaConsumerConfig = kafkaConsumerOptions.Value;
            ConsumerConfig = _kafkaConsumerConfig.ToConsumerConfig();

            if (_kafkaConsumerConfig.MakeGroupUnique)
                ConsumerConfig.GroupId += $"_{DateTime.Now}";

            _consumer = new ConsumerBuilder<TKey, TValue>(ConsumerConfig).Build();

            ProcessMessageAsync = processMessageAsync;

            _consumerTimer = new Timer(ConsumerTimerHandler, null, Timeout.Infinite, Timeout.Infinite);
            _consumingIsInProgress = 0;

            _disposed = 0;
            
            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        /// <summary>
        /// Start service
        /// </summary>
        /// <returns></returns>
        public Task Start()
        {
            foreach (string topicName in _kafkaConsumerConfig.Topics)
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

                        bool commitConsume = !_kafkaConsumerConfig.AutoCommit;

                        if(ProcessMessageAsync == null)
                        {
                            _messagesQueue.Enqueue(consumeResult.Message);
                            Interlocked.Increment(ref _messagesQueueLength);
                        }
                        else
                        {
                            try
                            {
                                if (!await ProcessMessageAsync(consumeResult.Message))
                                {
                                    commitConsume = false;
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Unhandled exception in Kafka message processing delegate.");
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
            int originalValue = Interlocked.CompareExchange(ref _disposed, 1, 0);
            
            if(originalValue != 0 || !disposing)
                return;

            _consumer?.Dispose();
            _cancellationTokenSource?.Dispose();
            _consumerTimer?.Dispose();
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

