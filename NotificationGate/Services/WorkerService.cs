using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NotificationGate.Services
{
    public class WorkerService
    {
        private readonly ProducerService _producer;
        private readonly ILogger<WorkerService> _logger;
        private readonly string _topicName;
        public WorkerService(
            ProducerService producer,
            IConfiguration config,
            ILogger<WorkerService> logger
            )
        {
            _producer = producer;
            _logger = logger;

            _topicName = config["KAFKA_TOPIC_RAW_ALERTS"]
                ?? throw new ArgumentException("Cannot find a topic name");
        }
        public void Execute(CancellationToken stoppingToken)
        {
            var basePath = Directory.GetCurrentDirectory();
            var alertPath = Path.Combine(basePath, "alert-simulator", "alerts");
            using var watcher = new FileSystemWatcher(alertPath);

            watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastAccess
                                 | NotifyFilters.LastWrite
                                 | NotifyFilters.Security
                                 | NotifyFilters.Size;

            watcher.Created += OnCreated;
            watcher.Error += OnError;

            watcher.Filter = "*.ready";
            watcher.IncludeSubdirectories = true;
            watcher.EnableRaisingEvents = true;

            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }

        private async void OnCreated(object sender, FileSystemEventArgs e)
        {
            string value = $"Created: {e.FullPath}";
            var parent = Directory.GetParent(e.FullPath).ToString();

            //Reading the file
            try
            {
                var jsonFile = Path.Combine(parent, "alert.json");
                var readFile = File.ReadAllText(jsonFile);
                await _producer.ProduceAlert(readFile, _topicName);
                _logger.LogInformation("Produce alert successfully");
            }
            catch(Exception ex)
            {
                _logger.LogError("Failed to read and produce the alert: {ex}", ex.Message);
            }
        }

        private void OnError(object sender, ErrorEventArgs e)
        {
            _logger.LogError(e.GetException().ToString());
        }

        private static void PrintException(Exception? ex)
        {
            if (ex != null)
            {
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine("Stacktrace:");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine();
                PrintException(ex.InnerException);
            }
        }
    }
}