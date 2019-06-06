using System;
using System.Diagnostics;
using Raven.Client.Documents;
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
                    new ResourcePropertyIndex().Execute(store);
                }

                if (store.Maintenance.Send(new GetIndexOperation("ResourceClusterIndex")) == null)
                {
                    new ResourceClusterIndex().Execute(store);
                }

                if (store.Maintenance.Send(new GetIndexOperation("ResourceDerivedPropertyIndex")) == null)
                {
                    new ResourceDerivedPropertyIndex().Execute(store);
                }

                if (store.Maintenance.Send(new GetIndexOperation("ResourceInversePropertyIndex")) == null)
                {
                    new ResourceInversePropertyIndex().Execute(store);
                }

                if (store.Maintenance.Send(new GetIndexOperation("ResourceReasonerIndex")) == null)
                {
                    new ResourceReasonerIndex().Execute(store);
                }

                stopwatch.Stop();
                Console.WriteLine(stopwatch.Elapsed);
            }
        }
    }
}
