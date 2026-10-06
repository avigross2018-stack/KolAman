using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using NorthConsumer.Models;
using SouthConsumer.Repo;


namespace SouthConsumer.Services
{
    public class WorkerService : BackgroundService
    {
        private readonly ConsumerService _consumer;
        private readonly SouthRepo _northRepo;
        
        public WorkerService(ConsumerService consumer, SouthRepo northRepo)
        {
            _consumer = consumer;
            _northRepo = northRepo;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _consumer.ConsumeMessage(stoppingToken, MessageHandler, "queue_SOUTH", "alert_exchange", "SOUTH");
            }
        }

        private async Task MessageHandler(byte[] bytes)
        {
            var message = Encoding.UTF8.GetString(bytes);

            var objModel = JsonSerializer.Deserialize<Alert>(message);
            await _northRepo.CreateAsync(objModel);
            await Task.CompletedTask;
        }
    }
}