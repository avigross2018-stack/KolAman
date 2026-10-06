
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.VisualBasic;
using NorthConsumer.Data;
using NorthConsumer.Repo;
using NorthConsumer.Services;
using RabbitMQ.Client;

var builder  = Host.CreateApplicationBuilder(args);

var rabbitHost = builder.Configuration["RABBITMQ_HOST"] ?? throw new ArgumentException("missing env var");
var rabbitPort = builder.Configuration["RABBITMQ_PORT"] ?? throw new ArgumentException("missing env var");
var rabbitUser = builder.Configuration["RABBITMQ_DEFAULT_USER"] ?? throw new ArgumentException("missing env var");
var rabbitPass = builder.Configuration["RABBITMQ_DEFAULT_PASS"] ?? throw new ArgumentException("missing env var");

var sqlConnection = builder.Configuration["MYSQL_CONNECTION"];
// var connection = builder.Configuration.GetConnectionString(sqlConnectionString);

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





builder.Services.AddScoped<NorthRepo>();
builder.Services.AddScoped<ConsumerService>();
builder.Services.AddHostedService<WorkerService>();

var host = builder.Build();

await host.RunAsync();