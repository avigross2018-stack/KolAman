using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CenterConsumer.Models;
using MongoDB.Driver;



namespace CenterConsumer.Repo
{
    public class CenterRepo
    {
        private readonly IMongoCollection<Alert> _collection;
        public CenterRepo(IMongoCollection<Alert> collection)
        {
            _collection = collection;
        }

        public async Task CreateAsync(Alert alert)
        {
            await _collection.InsertOneAsync(alert);
        }
    }
}