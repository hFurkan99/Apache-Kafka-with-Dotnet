using Kafka.Producer;
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

//await kafkaService.CreateTopicWithClusterAsync(topicNames);
await kafkaService.CreateTopicAsync(topicNames);
//await kafkaService.SendSimpleMessageWithNullKey(topicNames[0]);
//await kafkaService.SendSimpleMessageWithIntlKey(topicNames[1]);
//await kafkaService.SendComplexMessageWithIntlKey(topicNames[2]);
//await kafkaService.SendComplexMessageWithIntlKeyAndHeader(topicNames[3]);
//await kafkaService.SendComplexMessageWithComplexKey(topicNames[4]);
//await kafkaService.SendMessageWithTimestamp(topicNames[5]);
//await kafkaService.SendMessageToSpecificPartition(topicNames[6]);
//await kafkaService.SendMessageWithAck(topicNames[8]);
//await kafkaService.SendMessageToCluster(topicNames[10]);
await kafkaService.SendMessageWithRetry(topicNames[0]);

Console.WriteLine("Mesajlar Gönderilmiştir.");
