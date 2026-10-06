using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using MongoDB.Driver;
using NorthConsumer.Models;

namespace NorthConsumer.Repo
{
    public class NorthRepo
    {
        private readonly IMongoCollection<Alert> _collection;
        public NorthRepo(IMongoCollection<Alert> collection)
        {
            _collection = collection;
        }

        public async Task CreateAsync(Alert alert)
        {
            await _collection.InsertOneAsync(alert);
        }
    }
}