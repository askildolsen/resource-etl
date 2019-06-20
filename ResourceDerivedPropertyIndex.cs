using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceDerivedPropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourceDerivedPropertyIndex()
        {
            AddMap<ResourceCluster>(clusters =>
                from cluster in clusters.Where(r => r.Context == "@geohash" && r.Source.Skip(1).Any())
                let resources = LoadDocument<ResourceProperty>(cluster.Source).Where(r => r != null)

                let comparisons = 
                    from resource in resources
                    let ontology = resource.Properties.Where(p => p.Name == "@ontology").SelectMany(p => p.Resources)
                    select new Resource
                    {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Properties =
                            from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                            let propertyontology = ontology.SelectMany(r => r.Properties).Where(p => p.Name == property.Name)
                            where propertyontology.Any()
                            select new Property {
                                Name = property.Name,
                                Value = property.Value,
                                Resources =
                                    from resourcecompare in resources.Where(r => !(r.Context == resource.Context && r.ResourceId == resource.ResourceId))
                                    let ontologyresources = propertyontology.SelectMany(p => p.Resources)
                                    where ontologyresources.Any(r => r.Type.Any(t => resourcecompare.Properties.Where(p => p.Name == "@type").SelectMany(p => p.Value).Contains(t)))
                                    select new Resource {
                                        Context = resourcecompare.Context,
                                        ResourceId = resourcecompare.ResourceId,
                                        Properties =
                                            from propertycompare in resourcecompare.Properties.Where(p => p.Tags.Contains("@wkt"))
                                            select new Property {
                                                Name = propertycompare.Name,
                                                Value = propertycompare.Value
                                            }
                                    }
                            }
                    }

                from resource in (IEnumerable<Resource>)Intersects(comparisons)
                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Properties = resource.Properties,
                    Source = new string[] { },
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
                            Value = propertyG.SelectMany(p => p.Value).Distinct(),
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
            indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "128"} };

            return indexDefinition;
        }
    }
}
