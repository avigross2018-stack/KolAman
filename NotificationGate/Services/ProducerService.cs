using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace NotificationGate.Services
{
    public class ProducerService
    {
        private readonly IProducer<Null, string> _producer;
        public ProducerService(IProducer<Null, string> producer)
        {
            _producer = producer;
        }

        public async Task ProduceAlert(string alert, string topicName)
        {
            var message = new Message<Null, string>
            {
                Value = alert
            };

            await _producer.ProduceAsync(topicName, message);
        }

        public void Dispose()
        {
            _producer.Dispose();
            _producer.Flush();
        }
    }
}