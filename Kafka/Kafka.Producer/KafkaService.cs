using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer.Events;
using System.Globalization;
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
                        new TopicSpecification() {Name = topicName, NumPartitions = 1, ReplicationFactor = 1, Configs = configs }
                    ]);
                    Console.WriteLine($"Topic({topicName} oluştu.)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        internal async Task CreateTopicAsyncWithRetentionAsync(List<string> topicNames)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"
            }).Build();

  
            foreach (var topicName in topicNames)
            {
                try
                {
                    TimeSpan day30Span = TimeSpan.FromDays(30);

                    var configs = new Dictionary<string, string>()
                    {
                        {"message.timestamp.type", "LogAppendTime"},
                        //{"retention.ms", "-1" } // ömür boyu kafka'da sakla
                        //{"retention.bytes", "10000" } // byte cinsinden sakla 10kb
                        
                        {"retention.ms", day30Span.TotalMicroseconds.ToString(CultureInfo.InvariantCulture)}
                    };

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

        internal async Task CreateTopicWithClusterAsync(List<string> topicNames)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000, localhost:7001, localhost:7002"
            }).Build();

            var configs = new Dictionary<string, string>()
            {
                {"message.timestamp.type", "LogAppendTime"},
                {"min.insync.replicas", "3"}
            };

            foreach (var topicName in topicNames)
            {
                try
                {
                    await adminClient.CreateTopicsAsync(
                    [
                        new TopicSpecification() {Name = topicName, NumPartitions = 6, ReplicationFactor = 3, Configs = configs }
                    ]);

                    Console.WriteLine($"Topic({topicName} oluştu.)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        internal async Task CreateTopicRetryClusterAsync(List<string> topicNames)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000, localhost:7001, localhost:7002"
            }).Build();

            var configs = new Dictionary<string, string>()
            {
                {"message.timestamp.type", "LogAppendTime"},

            };

            foreach (var topicName in topicNames)
            {
                try
                {
                    await adminClient.CreateTopicsAsync(
                    [
                        new TopicSpecification() {Name = topicName, NumPartitions = 6, ReplicationFactor = 3, Configs = configs }
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

        internal async Task SendMessageWithAck(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.All };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {

                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj: {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendMessageToCluster(string topicName)
        {
            var config = new ProducerConfig() { BootstrapServers = "localhost:7000, localhost:7001, localhost:7002", Acks = Acks.All };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj: {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendMessageWithRetryToCluster(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7000, localhost:7001, localhost:7002",
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                //RetryBackoffMaxMs = 600000,
                //RetryBackoffMs = 600000
                MessageTimeoutMs = 5000
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 100))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj: {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }

        internal async Task SendMessageWithRetry(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094",
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                //RetryBackoffMaxMs = 600000,
                //RetryBackoffMs = 600000
                MessageTimeoutMs = 5000
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 1000))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj: {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("----------------------------------------------");
            }
        }
    }
}
