using Confluent.Kafka;
using Kafka.Consumer.Events;
using System.Text;

namespace Kafka.Consumer
{
    internal class KafkaService
    {
        internal void ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true) 
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj: {consumeResult.Message.Value}");
                }
            }
        }

        internal void ConsumeSimpleMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj: Key = {consumeResult.Message.Key} --- Value = {consumeResult.Message.Value}");
                }
            }
        }

        internal void ConsumeComplexMessageWithIntKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-3-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var orderCreatedEvent = consumeResult.Message.Value;
                    Console.WriteLine($"gelen mesaj: {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }
            }
        }

        internal void ConsumeComplexMessageWithIntKeyAndHeader(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-4-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlation_id"));
                    var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));

                    var orderCreatedEvent = consumeResult.Message.Value;
                    Console.WriteLine($"gelen mesaj: UserId={orderCreatedEvent.UserId} - OrderCode={orderCreatedEvent.OrderCode} - TotalPrice={orderCreatedEvent.TotalPrice}" +
                        $"- correlationId={correlationId} - version={version}");
                }
            }
        }

        internal void ConsumeComplexMessageWithComplexKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-5-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Message.Key;
                    
                    var orderCreatedEvent = consumeResult.Message.Value;
                    Console.WriteLine($"gelen mesaj: UserId={orderCreatedEvent.UserId} - OrderCode={orderCreatedEvent.OrderCode} - TotalPrice={orderCreatedEvent.TotalPrice}" +
                        $"- key1={messageKey.Key1} - key2={messageKey.Key2}");
                }
            }
        }

        internal void ConsumeMessageWithTimestamp(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-6-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>())
                .Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Message.Key;

                    var orderCreatedEvent = consumeResult.Message.Value;
                    Console.WriteLine($"gelen mesaj: UserId={orderCreatedEvent.UserId} - OrderCode={orderCreatedEvent.OrderCode} - TotalPrice={orderCreatedEvent.TotalPrice}" +
                        $"- key1={messageKey.Key1} - key2={messageKey.Key2} Message Timestamp={consumeResult.Message.Timestamp.UtcDateTime}");
                }
            }
        }

        internal void ConsumeMessageFromSpecificPartition(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-7-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Assign(new TopicPartition(topicName, new Partition(2)));

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj: {consumeResult.Message.Value}");
                }
            }
        }
        internal void ConsumeMessageFromSpecificPartitionOffset(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-8-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Assign(new TopicPartitionOffset(topicName, 2, 4));

            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj: {consumeResult.Message.Value}");
                }
            }
        }
    }
}
