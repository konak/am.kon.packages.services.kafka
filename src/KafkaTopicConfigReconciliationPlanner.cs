using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Confluent.Kafka.Admin;

namespace am.kon.packages.services.kafka
{
    internal static class KafkaTopicConfigReconciliationPlanner
    {
        internal const string MinInSyncReplicasConfigName = "min.insync.replicas";
        internal const string UncleanLeaderElectionConfigName = "unclean.leader.election.enable";

        private static readonly HashSet<string> SupportedConfigNames =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                MinInSyncReplicasConfigName,
                UncleanLeaderElectionConfigName,
            };

        public static Dictionary<string, string> ResolveDesiredConfigs(
            IReadOnlyDictionary<string, string> configuredValues)
        {
            var desired = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (configuredValues == null)
                return desired;

            foreach (var entry in configuredValues)
            {
                if (SupportedConfigNames.Contains(entry.Key))
                    desired[CanonicalName(entry.Key)] = entry.Value;
            }

            return desired;
        }

        public static List<ConfigEntry> ResolveAlterations(
            IReadOnlyDictionary<string, string> desiredValues,
            IReadOnlyDictionary<string, string> currentValues)
        {
            var current = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (currentValues != null)
            {
                foreach (var entry in currentValues)
                    current[entry.Key] = entry.Value;
            }

            return desiredValues
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Where(entry =>
                    !current.TryGetValue(entry.Key, out var currentValue) ||
                    !ValuesEqual(entry.Key, entry.Value, currentValue))
                .Select(entry => new ConfigEntry
                {
                    Name = entry.Key,
                    Value = entry.Value,
                    IncrementalOperation = AlterConfigOpType.Set,
                })
                .ToList();
        }

        public static void ValidateExistingReplicationFactor(
            string topicName,
            IReadOnlyDictionary<string, string> desiredValues,
            int existingReplicationFactor)
        {
            if (!desiredValues.TryGetValue(MinInSyncReplicasConfigName, out var value))
                return;

            if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var minInSyncReplicas))
                throw new ArgumentException($"Kafka topic '{topicName}' has invalid {MinInSyncReplicasConfigName} value '{value}'.");

            if (existingReplicationFactor <= 0)
                throw new InvalidOperationException($"Kafka topic '{topicName}' has no readable partition replica metadata.");

            if (minInSyncReplicas > existingReplicationFactor)
            {
                throw new InvalidOperationException(
                    $"Kafka topic '{topicName}' cannot reconcile {MinInSyncReplicasConfigName}={minInSyncReplicas} " +
                    $"because its existing replication factor is {existingReplicationFactor}. " +
                    "The topic manager does not change replica assignments.");
            }
        }

        private static bool ValuesEqual(string name, string desiredValue, string currentValue)
        {
            if (string.Equals(name, MinInSyncReplicasConfigName, StringComparison.OrdinalIgnoreCase) &&
                int.TryParse(desiredValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var desiredInteger) &&
                int.TryParse(currentValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var currentInteger))
            {
                return desiredInteger == currentInteger;
            }

            if (string.Equals(name, UncleanLeaderElectionConfigName, StringComparison.OrdinalIgnoreCase) &&
                bool.TryParse(desiredValue, out var desiredBoolean) &&
                bool.TryParse(currentValue, out var currentBoolean))
            {
                return desiredBoolean == currentBoolean;
            }

            return string.Equals(desiredValue, currentValue, StringComparison.Ordinal);
        }

        private static string CanonicalName(string name)
        {
            return string.Equals(name, MinInSyncReplicasConfigName, StringComparison.OrdinalIgnoreCase)
                ? MinInSyncReplicasConfigName
                : UncleanLeaderElectionConfigName;
        }
    }
}
