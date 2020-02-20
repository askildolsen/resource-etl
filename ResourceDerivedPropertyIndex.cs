using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceDerivedPropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourceDerivedPropertyIndex()
        {
            AddMap<ResourceCluster>(clusters =>
                from cluster in clusters.Where(r => r.Context == "@geohash")
                let parentClusters =
                    from i in Enumerable.Range(1, cluster.ResourceId.Length)
                    let parenthash = cluster.ResourceId.Substring(0, cluster.ResourceId.Length - i)
                    where parenthash.Length > 0
                    select LoadDocument<ResourceClusterReferences>("ResourceClusterReferences/@geohash/" + parenthash)

                let parentOutputs = LoadDocument<ResourceCluster>(parentClusters.Where(r => r != null).SelectMany(c => c.ReduceOutputs).Distinct()).Where(r => r != null)

                let resources = LoadDocument<ResourceProperty>(cluster.Source.Union(parentOutputs.SelectMany(p => p.Source)).Distinct()).Where(r => r != null)
                where resources.Take(1).Any()

                from resource in resources
                select new Resource {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Properties =
                        from property in resource.Properties.Where(p => p.Tags.Contains("@cluster:geohash"))
                        select new Property {
                            Name = property.Name,
                            Tags = property.Tags,
                            Resources =
                                from ontologyresource in property.Resources
                                from resourcecompare in resources.Where(r => !(r.Context == resource.Context && r.ResourceId == resource.ResourceId))
                                where ontologyresource.Context == resourcecompare.Context
                                    && ontologyresource.Type.All(type => resourcecompare.Type.Contains(type))
                                    && ontologyresource.Properties.Any(p1 => resourcecompare.Properties.Any(p2 => p1.Name == p2.Name))
                                select
                                    new Resource {
                                        Context = resourcecompare.Context,
                                        ResourceId = resourcecompare.ResourceId
                                    }
                        },
                    Source = new string[] { MetadataFor(resource).Value<String>("@id") },
                    Modified = MetadataFor(cluster).Value<DateTime>("@last-modified")
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId } into g
                select new Resource
                {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Properties = 
                        from property in g.SelectMany(resource => resource.Properties)
                        group property by property.Name into propertyG
                        select new Property {
                            Name = propertyG.Key,
                            Tags = propertyG.SelectMany(p => p.Tags).Distinct(),
                            Resources = propertyG.SelectMany(p => p.Resources).Distinct()
                        },
                    Source = g.SelectMany(resource => resource.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified).Max()
                };

            Index(r => r.Properties, FieldIndexing.No);
            Store(r => r.Properties, FieldStorage.Yes);

            OutputReduceToCollection = "ResourceDerivedProperty";

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
            indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "8192"} };

            return indexDefinition;
        }
    }
}
