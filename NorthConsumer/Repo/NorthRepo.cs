using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using NorthConsumer.Data;
using NorthConsumer.Models;

namespace NorthConsumer.Repo
{
    public class NorthRepo
    {
        private readonly NorthDbContext _northDb;
        public NorthRepo(NorthDbContext northDb)
        {
            _northDb = northDb;
        }

        public async Task Createdatbase()
        {
            await _northDb.Database.MigrateAsync();
        }

        public async Task AddAlert(Alert alert)
        {
            await _northDb.Alerts.AddAsync(alert);
            await _northDb.SaveChangesAsync();
        }
    }
}