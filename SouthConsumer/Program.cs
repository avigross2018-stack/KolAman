
using System.Runtime.InteropServices;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.VisualBasic;
using MongoDB.Driver;
using NorthConsumer.Models;

using RabbitMQ.Client;
using SouthConsumer.Repo;
using SouthConsumer.Services;

var builder  = Host.CreateApplicationBuilder(args);

var rabbitHost = builder.Configuration["RABBITMQ_HOST"] ?? throw new ArgumentException("missing env var");
var rabbitPort = builder.Configuration["RABBITMQ_PORT"] ?? throw new ArgumentException("missing env var");
var rabbitUser = builder.Configuration["RABBITMQ_DEFAULT_USER"] ?? throw new ArgumentException("missing env var");
var rabbitPass = builder.Configuration["RABBITMQ_DEFAULT_PASS"] ?? throw new ArgumentException("missing env var");

var mongoHost = builder.Configuration["MONGO_HOST"];
var mongoPort = builder.Configuration["MONGO_PORT"];
var mongoUser = builder.Configuration["MONGO_USER"];
var mongoPass = builder.Configuration["MONGO_PASSWORD"];
var mongoDatabase = builder.Configuration["MONGO_DATABASE"];


var mongoConnection = $"mongodb://{mongoUser}:{mongoPass}@{mongoHost}:{mongoPort}/?auth=admin";
builder.Services.AddSingleton<ConnectionFactory>(_ =>
{
    return new ConnectionFactory
    {
        HostName = rabbitHost,
        Port = int.Parse(rabbitPort),
        UserName = rabbitUser,
        Password = rabbitPass
    };
});

builder.Services.AddSingleton<IMongoCollection<Alert>>(_ =>
{
    var client = new MongoClient(mongoConnection);
    var database = client.GetDatabase(mongoDatabase);
    return database.GetCollection<Alert>("south-collection");
});



builder.Services.AddSingleton<SouthRepo>();
builder.Services.AddSingleton<ConsumerService>();
builder.Services.AddHostedService<WorkerService>();

var host = builder.Build();

await host.RunAsync();