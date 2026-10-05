
using Confluent.Kafka;
using Elastic.Serilog.Sinks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NotificationGate.Services;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

var kafkaBootstrap = builder.Configuration["KAFKA_BOOTSTRAP_SERVER"];
var elasticUrl = builder.Configuration["ELASTIC_BASE_URL"];

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.Elasticsearch(
        new [] { new Uri(elasticUrl)}, op =>
        {
            op.DataStream = new Elastic.Ingest.Elasticsearch.DataStreams.DataStreamName(
                "logs", "notification-gate", "dev"
            );
            op.BootstrapMethod = Elastic.Ingest.Elasticsearch.BootstrapMethod.Failure;
        }
    )
    .CreateLogger();

builder.Services.AddSerilog();
builder.Services.AddSingleton<IProducer<Null, string>>(_ =>
{
    var producerConfig = new ProducerConfig
    {
        BootstrapServers = kafkaBootstrap
    };
    return new ProducerBuilder<Null, string>(producerConfig).Build();
});

builder.Services.AddSingleton<ProducerService>();
builder.Services.AddSingleton<WorkerService>();

var provider = builder.Services.BuildServiceProvider();
var worker = provider.GetRequiredService<WorkerService>();

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = false;
    cts.Cancel();
};

try
{
    worker.Execute(cts.Token);
}
catch(Exception ex)
{
    Log.Error(ex.Message);
}
