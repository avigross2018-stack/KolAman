using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Driver;
using NorthConsumer.Models;


namespace SouthConsumer.Repo
{
    public class SouthRepo
    {
        private readonly IMongoCollection<Alert> _collection;
        public SouthRepo(IMongoCollection<Alert> collection)
        {
            _collection = collection;
        }

        public async Task CreateAsync(Alert alert)
        {
            await _collection.InsertOneAsync(alert);
        }
    }
}