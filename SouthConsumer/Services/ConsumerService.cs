using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace SouthConsumer.Services
{
    public class ConsumerService
    {
        private readonly ConnectionFactory _factory;

        private IConnection? _connection;
        private IChannel? _channel;
        public ConsumerService(ConnectionFactory factory)
        {
            _factory = factory;
        }

        private async Task CreateConnection(CancellationToken stopToken)
        {
            if(_connection is not null && _connection.IsOpen)
            {
                return;
            }

            _connection = await _factory.CreateConnectionAsync(stopToken);
            _channel = await _connection.CreateChannelAsync();
        }

        public async Task ConsumeMessage(
            CancellationToken stopToken, 
            Func<byte[], Task> msgHandler, 
            string queueName,
            string exchangeName,
            string routingName
            )
        {
            
            await CreateConnection(stopToken);

            await _channel!.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Direct,
                durable: true,
                cancellationToken: stopToken
            );

            await _channel!.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: stopToken
            );

            await _channel!.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: routingName
            );

            var consumer = new AsyncEventingBasicConsumer(_channel);

            consumer.ReceivedAsync += async (mod, arg) =>
            {

                try
                {
                    var body = arg.Body.ToArray();
                    var routing = arg.RoutingKey;
                    await msgHandler(body);

                    await _channel.BasicAckAsync(
                        deliveryTag: arg.DeliveryTag,
                        multiple: false,
                        cancellationToken: stopToken
                    );

                }
                catch
                {                  
                    await _channel.BasicNackAsync(
                        deliveryTag: arg.DeliveryTag,
                        multiple: false,
                        requeue: false,
                        cancellationToken: stopToken
                    );
                    
                }

            };

            await _channel.BasicConsumeAsync(
                    queue: queueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: stopToken
                );
        }
    }
}