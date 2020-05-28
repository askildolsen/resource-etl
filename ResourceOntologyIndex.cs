using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceOntologyIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourceOntologyIndex()
        {
            AddMapForAll<ResourceMapped>(resources =>
                from resource in resources.Where(r => MetadataFor(r).Value<String>("@collection").EndsWith("Resource"))
                let context = MetadataFor(resource).Value<String>("@collection").Replace("Resource", "")
                from type in resource.Type
                let ontology = LoadDocument<OntologyResource>("OntologyResource/" + context + "/" + type)
                where ontology != null
                select new Resource
                {
                    Context = context,
                    ResourceId = type + "/" + GenerateHash(resource.ResourceId),
                    Properties = ontology.Properties,
                    Source = new[] { MetadataFor(resource).Value<String>("@id") },
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId } into g
                select new Resource
                {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Properties = g.SelectMany(resource => resource.Properties).Distinct(),
                    Source = g.SelectMany(resource => resource.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified).Max()
                };

            Index(r => r.Properties, FieldIndexing.No);
            Store(r => r.Properties, FieldStorage.Yes);

            OutputReduceToCollection = "ResourceOntology";

            AdditionalSources = new Dictionary<string, string>
            {
                {
                    "ResourceModelUtils",
                    ReadResourceFile("resource_etl.ResourceModelUtils.cs")
                }
            };
        }
    }
}
