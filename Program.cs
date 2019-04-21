﻿using System;
using System.Diagnostics;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.Indexes;

namespace resource_etl
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore { Urls = new string[] { "http://localhost:8080" }, Database = "Digitalisert" })
            {
                store.Conventions.FindCollectionName = t => t.Name;
                store.Initialize();

                var stopwatch = Stopwatch.StartNew();

                if (store.Maintenance.Send(new GetIndexOperation("ResourcePropertyIndex")) == null)
                {
                    new ResourceModel.ResourcePropertyIndex().Execute(store);
                }

                if (store.Maintenance.Send(new GetIndexOperation("ResourceReasonerIndex")) == null)
                {
                    new ResourceModel.ResourceReasonerIndex().Execute(store);
                }

                stopwatch.Stop();
                Console.WriteLine(stopwatch.Elapsed);
            }
        }
    }
}
