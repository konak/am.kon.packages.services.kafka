using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using am.kon.packages.services.kafka.Config;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace am.kon.packages.services.kafka
{
    /// <summary>
    /// Service responsible for managing Kafka topics, ensuring their existence and handling creation logic.
    /// This service interacts with the Kafka cluster for administrative tasks like creating topics and monitoring their status.
    /// </summary>
    public class KafkaTopicManagerService
    {
        private readonly ILogger<KafkaTopicManagerService> _logger;
        private readonly KafkaTopicManagerConfig _config;
        private readonly AdminClientConfig _adminClientConfig;
        private readonly IAdminClient _adminClient;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        private int _disposed;
        private volatile bool _topicsCreated;


        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaTopicManagerService"/> class.
        /// This service is responsible for managing Kafka topics and ensuring their existence in the Kafka cluster.
        /// </summary>
        /// <param name="logger">The logger used for logging debug and error information.</param>
        /// <param name="configOptions">Configuration options for Kafka topic management, provided via dependency injection.</param>
        public KafkaTopicManagerService(
            ILogger<KafkaTopicManagerService> logger,
            IOptions<KafkaTopicManagerConfig> configOptions
            )
        {
            _logger = logger;
            _config = configOptions.Value;

            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = _config.BootstrapServers,
            };

            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            _topicsCreated = false;
            _disposed = 0;

            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        /// <summary>
        /// Starts the Kafka topic management service by triggering the creation of necessary Kafka topics.
        /// This method initiates the topic creation process, ensuring all required topics are present in the Kafka cluster.
        /// </summary>
        /// <returns>A task representing the asynchronous operation of starting the Kafka topic management service.</returns>
        public Task Start()
        {
            return CreateTopics();
        }

        /// <summary>
        /// Stops the service by canceling any ongoing operations and releasing resources.
        /// </summary>
        /// <returns>A task representing the asynchronous operation of stopping the service.</returns>
        public Task Stop()
        {
            _cancellationTokenSource.Cancel();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Waits for Kafka topics to be successfully created before proceeding with other operations.
        /// Continuously checks the topic creation status until either the topics are created or a cancellation is requested.
        /// </summary>
        /// <returns>A Task that represents the asynchronous operation, completing when the topics are successfully created or the operation is canceled.</returns>
        public async Task WaitForTopicsCreation()
        {
            while (!_topicsCreated && !_cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(500);
            }
        }

        /// <summary>
        /// Ensures the creation and existence of configured Kafka topics in the Kafka cluster.
        /// This method continuously attempts to create topics if they do not already exist, until successful
        /// or the operation is canceled via a cancellation token.
        /// </summary>
        /// <returns>A task representing the asynchronous operation of creating the required Kafka topics.</returns>
        public async Task CreateTopics()
        {
            while (!_topicsCreated && !_cancellationToken.IsCancellationRequested)
            {
                bool topicsCreationError = false;

                foreach (var topicName in _config.EnsureExistTopics)
                {
                    try
                    {
                        var metadata = _adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));

                        var topicExists = metadata.Topics.Any(t => t.Topic == topicName && t.Error.Code == ErrorCode.NoError);

                        // if topic exist continue to next topic
                        if (topicExists)
                        {
                            continue;
                        }

                        // crete topic
                        await _adminClient.CreateTopicsAsync(new[]
                        {
                            new TopicSpecification
                            {
                                Name = topicName,
                                NumPartitions = _config.NumPartitionsDefault,
                                ReplicationFactor = _config.ReplicationFactorDefault,
                            }
                        });
                    }
                    catch (CreateTopicsException ex)
                    {
                        if (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
                        {
                            continue;
                        }
                        else
                        {
                            _logger.LogError(ex, "CreateTopicsException exception in Kafka topic creation.");
                            topicsCreationError = true;
                        }

                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unhandled exception in Kafka topic creation.");
                        topicsCreationError = true;

                        break;
                    }
                }

                if (topicsCreationError)
                {
                    await Task.Delay(3000);
                }
                else
                {
                    _topicsCreated = true;
                }
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

            if(originalValue != 0)
                return;

            _adminClient?.Dispose();
            _cancellationTokenSource?.Dispose();
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
