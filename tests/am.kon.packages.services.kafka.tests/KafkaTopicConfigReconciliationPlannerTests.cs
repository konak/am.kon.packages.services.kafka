using am.kon.packages.services.kafka.Config;
using Confluent.Kafka.Admin;

namespace am.kon.packages.services.kafka.tests;

public sealed class KafkaTopicConfigReconciliationPlannerTests
{
    [Fact]
    public void ReconciliationOption_WhenOmitted_PreservesCreationOnlyDefault()
    {
        Assert.False(new KafkaTopicManagerConfig().ReconcileExistingTopicConfigs);
    }

    [Fact]
    public void ResolveDesiredConfigs_SelectsOnlySupportedDurabilityConfigs()
    {
        var desired = KafkaTopicConfigReconciliationPlanner.ResolveDesiredConfigs(
            new Dictionary<string, string>
            {
                ["min.insync.replicas"] = "2",
                ["unclean.leader.election.enable"] = "false",
                ["cleanup.policy"] = "compact",
            });

        Assert.Equal(2, desired.Count);
        Assert.Equal("2", desired["min.insync.replicas"]);
        Assert.Equal("false", desired["unclean.leader.election.enable"]);
        Assert.DoesNotContain("cleanup.policy", desired.Keys);
    }

    [Fact]
    public void ResolveDesiredConfigs_WhenConfigsAreOmitted_ReturnsEmptyPlan()
    {
        Assert.Empty(KafkaTopicConfigReconciliationPlanner.ResolveDesiredConfigs(null!));
    }

    [Fact]
    public void ResolveAlterations_WhenValuesMatch_IsIdempotent()
    {
        var desired = DurabilityConfigs();
        var alterations = KafkaTopicConfigReconciliationPlanner.ResolveAlterations(
            desired,
            new Dictionary<string, string>
            {
                ["min.insync.replicas"] = "2",
                ["unclean.leader.election.enable"] = "false",
            });

        Assert.Empty(alterations);
    }

    [Fact]
    public void ResolveAlterations_WhenValuesDrift_UsesIncrementalSetForSupportedConfigs()
    {
        var alterations = KafkaTopicConfigReconciliationPlanner.ResolveAlterations(
            DurabilityConfigs(),
            new Dictionary<string, string>
            {
                ["min.insync.replicas"] = "1",
                ["unclean.leader.election.enable"] = "true",
            });

        Assert.Equal(2, alterations.Count);
        Assert.All(alterations, alteration => Assert.Equal(AlterConfigOpType.Set, alteration.IncrementalOperation));
        Assert.Contains(alterations, alteration => alteration.Name == "min.insync.replicas" && alteration.Value == "2");
        Assert.Contains(alterations, alteration => alteration.Name == "unclean.leader.election.enable" && alteration.Value == "false");
    }

    [Fact]
    public void ResolveAlterations_WhenOneValueDrifts_ChangesOnlyThatValue()
    {
        var alteration = Assert.Single(KafkaTopicConfigReconciliationPlanner.ResolveAlterations(
            DurabilityConfigs(),
            new Dictionary<string, string>
            {
                ["min.insync.replicas"] = "2",
                ["unclean.leader.election.enable"] = "true",
            }));

        Assert.Equal("unclean.leader.election.enable", alteration.Name);
        Assert.Equal("false", alteration.Value);
        Assert.Equal(AlterConfigOpType.Set, alteration.IncrementalOperation);
    }

    [Fact]
    public void ResolveAlterations_WhenCurrentValueIsMissing_SetsDesiredValue()
    {
        var alteration = Assert.Single(KafkaTopicConfigReconciliationPlanner.ResolveAlterations(
            new Dictionary<string, string> { ["min.insync.replicas"] = "2" },
            new Dictionary<string, string>()));

        Assert.Equal("min.insync.replicas", alteration.Name);
        Assert.Equal("2", alteration.Value);
    }

    [Theory]
    [InlineData("FALSE")]
    [InlineData("False")]
    public void ResolveAlterations_BooleanComparisonIsCanonical(string currentValue)
    {
        var alterations = KafkaTopicConfigReconciliationPlanner.ResolveAlterations(
            new Dictionary<string, string> { ["unclean.leader.election.enable"] = "false" },
            new Dictionary<string, string> { ["unclean.leader.election.enable"] = currentValue });

        Assert.Empty(alterations);
    }

    [Fact]
    public void ValidateExistingReplicationFactor_WhenMinIsrFits_DoesNotThrow()
    {
        KafkaTopicConfigReconciliationPlanner.ValidateExistingReplicationFactor(
            "orders.events.v1",
            new Dictionary<string, string> { ["min.insync.replicas"] = "2" },
            3);
    }

    [Fact]
    public void ValidateExistingReplicationFactor_WhenMinIsrExceedsLiveReplication_ThrowsWithoutPlan()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            KafkaTopicConfigReconciliationPlanner.ValidateExistingReplicationFactor(
                "orders.events.v1",
                new Dictionary<string, string> { ["min.insync.replicas"] = "3" },
                2));

        Assert.Contains("does not change replica assignments", exception.Message);
    }

    private static Dictionary<string, string> DurabilityConfigs()
    {
        return new Dictionary<string, string>
        {
            ["min.insync.replicas"] = "2",
            ["unclean.leader.election.enable"] = "false",
        };
    }
}
