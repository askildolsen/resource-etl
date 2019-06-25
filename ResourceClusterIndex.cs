using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceClusterIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourceClusterIndex()
        {
            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties.Where(p => p.Tags.Contains("@wkt") && p.Tags.Contains("@cluster") && p.Tags.Any(t => t.StartsWith("@geohash")))
                from type in resource.Properties.Where(p => p.Name == "@type").SelectMany(p => p.Value).Distinct()
                from wkt in property.Value.Where(v => v != null)
                from geohash in WKTEncodeGeohash(wkt, 4)
                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = type + "/" + property.Name + "/" + geohash,
                    Source = new[] { MetadataFor(resource).Value<String>("@id")},
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties.Where(p => p.Tags.Contains("@wkt") && p.Tags.Contains("@cluster") && p.Tags.Any(t => t.StartsWith("@geohash")))
                from wkt in property.Value.Where(v => v != null)
                from inverseproperty in property.Properties.Where(p => p.Tags.Contains("@inverse"))
                from inverseresource in inverseproperty.Resources
                from inverseresourcetype in inverseresource.Type
                from geohash in WKTEncodeGeohash(wkt, 4)
                select new Resource
                {
                    Context = inverseresource.Context,
                    ResourceId = inverseresourcetype + "/" + inverseproperty.Name + "/" + geohash,
                    Source = new[] { MetadataFor(resource).Value<String>("@id")},
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
                    Source = g.SelectMany(r => r.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified).Max()
                };

            OutputReduceToCollection = "ResourceCluster";

            AdditionalSources = new Dictionary<string, string>
            {
                {
                    "ResourceModelUtils",
                    ReadResourceFile("resource_etl.ResourceModelUtils.cs")
                }
            };
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            var indexDefinition = base.CreateIndexDefinition();
            indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "256"} };

            return indexDefinition;
        }
    }
}
