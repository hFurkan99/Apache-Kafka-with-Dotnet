namespace Kafka.Consumer.Events
{
    internal record OrderCreatedEvent
    {
        public string OrderCode { get; init; }
        public decimal TotalPrice { get; init; }
        public long UserId { get; init; }
    }
}
