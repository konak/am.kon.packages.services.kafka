namespace am.kon.packages.services.kafka.Config
{
    public class KafkaTopicManagerConfig
    {
        public const string SectionDefaultName = "KafkaTopicManager";
        
        public string BootstrapServers { get; set; }

        public string[] EnsureExistTopics { get; set; }
        
        public int NumPartitionsDefault { get; set; }
        
        public short ReplicationFactorDefault { get; set; }
    }
}