using System;
using System.Collections.Generic;
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
        protected readonly ILogger<KafkaTopicManagerService> _logger;
        protected readonly KafkaTopicManagerConfig _config;
        protected readonly AdminClientConfig _adminClientConfig;
        private readonly IAdminClient _adminClient;

        protected readonly CancellationTokenSource _cancellationTokenSource;
        protected readonly CancellationToken _cancellationToken;

        protected int _disposed;
        private volatile bool _topicsCreated;
        private int _configLogged;


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
            _configLogged = 0;

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
            var ensureTopics = ResolveEnsureTopics();
            LogConfiguration(ensureTopics);

            if (ensureTopics.Length == 0)
            {
                _logger.LogWarning("EnsureExistTopics is empty. Kafka topic creation skipped.");
                _topicsCreated = true;
                return;
            }

            while (!_topicsCreated && !_cancellationToken.IsCancellationRequested)
            {
                bool topicsCreationError = false;

                foreach (var topicName in ensureTopics)
                {
                    try
                    {
                        var topicExists = TopicExists(topicName);

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
                        _logger.LogInformation("Kafka topic created: {TopicName}", topicName);
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

                if (!topicsCreationError)
                {
                    if (!AreTopicsReady(ensureTopics, out var missingTopics))
                    {
                        topicsCreationError = true;
                        _logger.LogWarning(
                            "Kafka topics not ready yet. Missing: {MissingTopics}",
                            string.Join(", ", missingTopics));
                    }
                }

                if (topicsCreationError)
                {
                    _logger.LogWarning("Retrying Kafka topic creation in 3 seconds.");
                    await Task.Delay(3000);
                }
                else
                {
                    _topicsCreated = true;
                }
            }
        }

        private string[] ResolveEnsureTopics()
        {
            if (_config.EnsureExistTopics == null || _config.EnsureExistTopics.Length == 0)
                return Array.Empty<string>();

            return _config.EnsureExistTopics
                .Where(topic => !string.IsNullOrWhiteSpace(topic))
                .Select(topic => topic.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        private void LogConfiguration(string[] ensureTopics)
        {
            if (Interlocked.CompareExchange(ref _configLogged, 1, 0) != 0)
                return;

            var topics = ensureTopics.Length == 0 ? "<none>" : string.Join(", ", ensureTopics);
            _logger.LogInformation(
                "Kafka topic manager starting. BootstrapServers={BootstrapServers}. EnsureExistTopics={Topics}.",
                _config.BootstrapServers,
                topics);
        }

        private bool AreTopicsReady(string[] ensureTopics, out List<string> missingTopics)
        {
            missingTopics = new List<string>();
            foreach (var topicName in ensureTopics)
            {
                if (!TopicExists(topicName))
                {
                    missingTopics.Add(topicName);
                }
            }

            return missingTopics.Count == 0;
        }

        private bool TopicExists(string topicName)
        {
            try
            {
                var metadata = _adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
                return metadata.Topics.Any(t =>
                    string.Equals(t.Topic, topicName, StringComparison.OrdinalIgnoreCase) &&
                    t.Error.Code == ErrorCode.NoError);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read Kafka metadata for topic {TopicName}.", topicName);
                return false;
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
