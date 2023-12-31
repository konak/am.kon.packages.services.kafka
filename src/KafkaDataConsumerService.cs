using System;
using am.kon.packages.services.kafka.Extensions;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;

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

        public KafkaDataConsumerService(
            ILogger<KafkaDataConsumerService<TKey, TValue>> logger,
            IConfiguration configuration,

            IOptions<KafkaConsumerConfig> kafkaConsumerOptions
            )
        {
            _logger = logger;
            _configuration = configuration;

            _messagesQueue = new ConcurrentQueue<Message<TKey, TValue>>();
            _messagesQueueLength = 0;

            _kafkaCoonsumerConfig = kafkaConsumerOptions.Value;
            _consumerConfig = _kafkaCoonsumerConfig.ToConsumerConfig();

            if (_kafkaCoonsumerConfig.MakeGroupUnique)
                _consumerConfig.GroupId += $"_{DateTime.Now.ToString()}";

            _consumer = new ConsumerBuilder<TKey, TValue>(_consumerConfig).Build();

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

                        _messagesQueue.Enqueue(consumeResult.Message);
                        Interlocked.Increment(ref _messagesQueueLength);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Consume error");
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
        /// Tries to get message from arrived messages queue
        /// </summary>
        /// <param name="message">Instance of arrived message</param>
        /// <returns></returns>
        public bool TryGetMessage(out Message<TKey, TValue> message)
        {
            if (_messagesQueue.TryDequeue(out message))
            {
                Interlocked.Decrement(ref _messagesQueueLength);
                return true;
            }

            return false;
        }
    }
}

