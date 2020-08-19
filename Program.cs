using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using static resource_etl.ResourceModel;

namespace resource_etl
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore { Urls = new string[] { "http://localhost:8080" }, Database = "Digitalisert" })
            {
                store.Conventions.FindCollectionName = t => t.Name.Replace("References", "/References");
                store.Conventions.Serialization = new NewtonsoftJsonSerializationConventions { CustomizeJsonSerializer = s => s.NullValueHandling = NullValueHandling.Ignore };
                store.Initialize();

                var stopwatch = Stopwatch.StartNew();

                using (BulkInsertOperation bulkInsert = store.BulkInsert())
                {
                    foreach(var resource in ResourceModel.Ontology)
                    {
                        foreach(var type in resource.Type)
                        {
                            bulkInsert.Store(
                                new Resource {
                                    Context = resource.Context,
                                    Type = resource.Type,
                                    Properties =
                                        from property in resource.Properties ?? new Property[] { }
                                        select new Property {
                                            Name = property.Name,
                                            Value = property.Value,
                                            Tags = property.Tags,
                                            Resources = property.Resources,
                                            Properties = (property.Properties ?? new Property[] { }).Union(
                                                from inverseresource in ResourceModel.Ontology
                                                from inverseproperty in (inverseresource.Properties ?? new Property[] { }).Where(p => p.Resources != null)
                                                from inversepropertyresource in inverseproperty.Resources.Where(r => r.Context == resource.Context && (r.Type ?? new string[] { }).Any(t => resource.Type.Contains(t)))
                                                where inversepropertyresource.Properties != null && inversepropertyresource.Properties.Any(p => p.Name == property.Name)
                                                select new Property {
                                                    Name = inverseproperty.Name,
                                                    Tags = new[] { "@inverse" }.Union(inverseproperty.Tags ?? new string[] { }),
                                                    Resources = new[] {
                                                        new Resource {
                                                            Context = inverseresource.Context,
                                                            Type = inverseresource.Type
                                                        }
                                                    }
                                                }
                                            )
                                        }
                                },
                                "OntologyResource/" + resource.Context + "/" + type,
                                new MetadataAsDictionary(new Dictionary<string, object> { { "@collection", "OntologyResource"}})
                            );
                        }
                    }
                }

                new ResourceOntologyIndex().Execute(store);
                new ResourcePropertyIndex().Execute(store);
                new ResourceClusterIndex().Execute(store);
                new ResourceDerivedPropertyIndex().Execute(store);
                new ResourceReasonerIndex().Execute(store);

                stopwatch.Stop();
                Console.WriteLine(stopwatch.Elapsed);
            }
        }
    }
}
