using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using am.kon.packages.services.kafka.Config;
using Confluent.Kafka.Admin;

namespace am.kon.packages.services.kafka
{
    internal static class KafkaTopicSpecificationResolver
    {
        private const string MinInSyncReplicasConfigName = "min.insync.replicas";
        private const string UncleanLeaderElectionConfigName = "unclean.leader.election.enable";

        public static TopicSpecification[] Resolve(KafkaTopicManagerConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            var resolved = new Dictionary<string, TopicSpecification>(StringComparer.OrdinalIgnoreCase);
            var defaultConfigs = NormalizeConfigs(config.TopicConfigsDefault, nameof(config.TopicConfigsDefault));

            foreach (var topicName in ResolveLegacyTopicNames(config.EnsureExistTopics))
            {
                resolved.Add(
                    topicName,
                    CreateSpecification(
                        topicName,
                        config.NumPartitionsDefault,
                        config.ReplicationFactorDefault,
                        defaultConfigs,
                        null));
            }

            var detailedSpecifications = new Dictionary<string, TopicSpecification>(StringComparer.OrdinalIgnoreCase);
            foreach (var topicConfig in config.EnsureExistTopicSpecifications ?? Array.Empty<KafkaTopicCreationConfig>())
            {
                if (topicConfig == null)
                    throw new ArgumentException("Detailed Kafka topic specifications cannot contain null entries.", nameof(config));

                var topicName = RequireTopicName(topicConfig.Name);
                var specification = CreateSpecification(
                    topicName,
                    topicConfig.NumPartitions ?? config.NumPartitionsDefault,
                    topicConfig.ReplicationFactor ?? config.ReplicationFactorDefault,
                    defaultConfigs,
                    topicConfig.Configs);

                if (detailedSpecifications.TryGetValue(topicName, out var existing))
                {
                    var errorKind = AreEquivalent(existing, specification) ? "Duplicate" : "Conflicting";
                    throw new ArgumentException(
                        $"{errorKind} detailed Kafka topic specification for '{topicName}'.",
                        nameof(config));
                }

                detailedSpecifications.Add(topicName, specification);

                // A detailed definition intentionally replaces the legacy definition with
                // the same name, providing a backward-compatible migration path.
                resolved[topicName] = specification;
            }

            return resolved.Values.ToArray();
        }

        private static IEnumerable<string> ResolveLegacyTopicNames(IEnumerable<string> topicNames)
        {
            if (topicNames == null)
                return Array.Empty<string>();

            return topicNames
                .Where(topic => !string.IsNullOrWhiteSpace(topic))
                .Select(topic => topic.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase);
        }

        private static TopicSpecification CreateSpecification(
            string topicName,
            int numPartitions,
            short replicationFactor,
            IReadOnlyDictionary<string, string> defaultConfigs,
            IDictionary<string, string> topicConfigs)
        {
            if (numPartitions <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(numPartitions),
                    numPartitions,
                    $"Kafka topic '{topicName}' must have a positive partition count.");
            }

            if (replicationFactor <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(replicationFactor),
                    replicationFactor,
                    $"Kafka topic '{topicName}' must have a positive replication factor.");
            }

            var configs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in defaultConfigs)
                configs.Add(entry.Key, entry.Value);

            foreach (var entry in NormalizeConfigs(topicConfigs, $"topic '{topicName}' configs"))
                configs[entry.Key] = entry.Value;

            ValidateAndNormalizeDurabilityConfigs(topicName, replicationFactor, configs);

            return new TopicSpecification
            {
                Name = topicName,
                NumPartitions = numPartitions,
                ReplicationFactor = replicationFactor,
                Configs = configs.Count == 0 ? null : configs,
            };
        }

        private static Dictionary<string, string> NormalizeConfigs(
            IEnumerable<KeyValuePair<string, string>> configs,
            string configSource)
        {
            var normalized = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (configs == null)
                return normalized;

            foreach (var entry in configs)
            {
                if (string.IsNullOrWhiteSpace(entry.Key))
                    throw new ArgumentException($"{configSource} contains an empty Kafka config key.");

                if (entry.Value == null)
                    throw new ArgumentException($"Kafka config '{entry.Key}' in {configSource} has a null value.");

                var key = entry.Key.Trim();
                var value = entry.Value.Trim();
                if (normalized.TryGetValue(key, out var existing) && !string.Equals(existing, value, StringComparison.Ordinal))
                    throw new ArgumentException($"{configSource} contains conflicting values for Kafka config '{key}'.");

                normalized[key] = value;
            }

            return normalized;
        }

        private static void ValidateAndNormalizeDurabilityConfigs(
            string topicName,
            short replicationFactor,
            IDictionary<string, string> configs)
        {
            if (configs.TryGetValue(MinInSyncReplicasConfigName, out var configuredValue))
            {
                if (!int.TryParse(configuredValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minInSyncReplicas) ||
                    minInSyncReplicas <= 0)
                {
                    throw new ArgumentException(
                        $"Kafka topic '{topicName}' has invalid {MinInSyncReplicasConfigName} value '{configuredValue}'.");
                }

                if (minInSyncReplicas > replicationFactor)
                {
                    throw new ArgumentException(
                        $"Kafka topic '{topicName}' has {MinInSyncReplicasConfigName}={minInSyncReplicas}, " +
                        $"which exceeds replication factor {replicationFactor}.");
                }

                SetCanonicalConfigValue(
                    configs,
                    MinInSyncReplicasConfigName,
                    minInSyncReplicas.ToString(CultureInfo.InvariantCulture));
            }

            if (configs.TryGetValue(UncleanLeaderElectionConfigName, out var uncleanLeaderElectionValue))
            {
                if (!bool.TryParse(uncleanLeaderElectionValue, out var uncleanLeaderElectionEnabled))
                {
                    throw new ArgumentException(
                        $"Kafka topic '{topicName}' has invalid {UncleanLeaderElectionConfigName} value " +
                        $"'{uncleanLeaderElectionValue}'.");
                }

                SetCanonicalConfigValue(
                    configs,
                    UncleanLeaderElectionConfigName,
                    uncleanLeaderElectionEnabled ? "true" : "false");
            }
        }

        private static void SetCanonicalConfigValue(
            IDictionary<string, string> configs,
            string canonicalName,
            string value)
        {
            var existingName = configs.Keys.First(key =>
                string.Equals(key, canonicalName, StringComparison.OrdinalIgnoreCase));
            if (!string.Equals(existingName, canonicalName, StringComparison.Ordinal))
                configs.Remove(existingName);

            configs[canonicalName] = value;
        }

        private static string RequireTopicName(string topicName)
        {
            if (string.IsNullOrWhiteSpace(topicName))
                throw new ArgumentException("Detailed Kafka topic specifications require a non-empty topic name.");

            return topicName.Trim();
        }

        private static bool AreEquivalent(TopicSpecification left, TopicSpecification right)
        {
            return left.NumPartitions == right.NumPartitions &&
                   left.ReplicationFactor == right.ReplicationFactor &&
                   ConfigsEqual(left.Configs, right.Configs);
        }

        private static bool ConfigsEqual(
            IReadOnlyDictionary<string, string> left,
            IReadOnlyDictionary<string, string> right)
        {
            if (left == null || left.Count == 0)
                return right == null || right.Count == 0;

            if (right == null || left.Count != right.Count)
                return false;

            return left.All(entry =>
                right.TryGetValue(entry.Key, out var value) &&
                string.Equals(entry.Value, value, StringComparison.Ordinal));
        }
    }
}
