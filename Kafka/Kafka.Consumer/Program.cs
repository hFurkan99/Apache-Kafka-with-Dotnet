using Kafka.Consumer;

var kafkaService = new KafkaService();

var topicNames = new List<string>
{
    //"use-case-1-topic",
    //"use-case-2-topic",
    //"use-case-3-topic",
    //"use-case-4-topic",
    //"use-case-5-topic",
    //"use-case-6-topic",
    //"use-case-7-topic",
    //"ack-topic",
    //"retention-topic",
    //"retention-topic-2",
    //"cluster-topic",
    "retry-topic"
};

//kafkaService.ConsumeSimpleMessageWithNullKey(topicNames[0]);
//kafkaService.ConsumeSimpleMessageWithIntKey(topicNames[1]);
//kafkaService.ConsumeComplexMessageWithIntKey(topicNames[2]);
//kafkaService.ConsumeComplexMessageWithIntKeyAndHeader(topicNames[3]);
//kafkaService.ConsumeComplexMessageWithComplexKey(topicNames[4]);
//kafkaService.ConsumeMessageWithTimestamp(topicNames[5]);
//kafkaService.ConsumeMessageFromSpecificPartition(topicNames[6]);
//kafkaService.ConsumeMessageFromSpecificPartitionOffset(topicNames[6]);
//kafkaService.ConsumeMessageWithAck(topicNames[7]);
kafkaService.ConsumeMessageWithRetry(topicNames[0]);

Console.ReadLine();