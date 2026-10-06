using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using NorthConsumer.Models;
using NorthConsumer.Repo;

namespace NorthConsumer.Services
{
    public class WorkerService : BackgroundService
    {
        private readonly ConsumerService _consumer;
        private readonly NorthRepo _northRepo;
        
        public WorkerService(ConsumerService consumer, NorthRepo northRepo)
        {
            _consumer = consumer;
            _northRepo = northRepo;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _northRepo.Createdatbase();
            while (!stoppingToken.IsCancellationRequested)
            {
                await _consumer.ConsumeMessage(stoppingToken, MessageHandler, "queue_NORTH", "alert_exchange", "NORTH");
            }
        }

        private async Task MessageHandler(byte[] bytes)
        {
            var message = Encoding.UTF8.GetString(bytes);

            var objModel = JsonSerializer.Deserialize<Alert>(message);
            await _northRepo.AddAlert(objModel);
            await Task.CompletedTask;
        }
    }
}