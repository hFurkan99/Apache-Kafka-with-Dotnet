using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer.Events;
using System.Text;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        internal async Task CreateTopicAsync(List<string> topicNames)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

            var configs = new Dictionary<string, string>()
            {
                {"message.timestamp.type", "LogAppendTime"}
            };

            foreach (var topicName in topicNames)
            {
                try
                {
                    await adminClient.CreateTopicsAsync(
                    [
                        new TopicSpecification() {Name = topicName, NumPartitions = 6, ReplicationFactor = 1, Configs = configs }
                    ]);
                    Console.WriteLine($"Topic({topicName} oluştu.)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        internal async Task SendSimpleMessageWithNullKey(string topicName) 
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Message(use case - 1) - {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendSimpleMessageWithIntlKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<int, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<int, string>()
                {
                    Value = $"Message(use case - 2) - {item}",
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendComplexMessageWithIntlKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item
                };

                //var newOrderCreatedEvent = orderCreatedEvent with { TotalPrice = 200 };

                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendComplexMessageWithIntlKeyAndHeader(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item
                };

                var headers = new Headers
                {
                    { "correlation_id", Encoding.UTF8.GetBytes("123")},
                    { "version", Encoding.UTF8.GetBytes("v1")},
                };

                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item,
                    Headers = headers
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendComplexMessageWithComplexKey(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item
                };

                var message = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = new MessageKey("key11 value", "key22 value")
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendMessageWithTimestamp(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
                .SetKeySerializer(new CustomKeySerializer<MessageKey>())
                .Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 100,
                    UserId = item
                };

                var message = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = new MessageKey("key11 value", "key22 value")
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendMessageToSpecificPartition(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj: {item}"
                };

                var topicPartition = new TopicPartition(topicName, new Partition(2));

                var result = await producer.ProduceAsync(topicPartition, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }
    }
}
