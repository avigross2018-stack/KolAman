using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using MongoDB.Driver;
using OverseasConsumer.Models;


namespace OverseasConsumer.Repo
{
    public class OverseasRepo
    {
        private readonly IMongoCollection<Alert> _collection;
        public OverseasRepo(IMongoCollection<Alert> collection)
        {
            _collection = collection;
        }

        public async Task CreateAsync(Alert alert)
        {
            await _collection.InsertOneAsync(alert);
        }
    }
}