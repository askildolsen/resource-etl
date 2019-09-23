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
                from cluster in clusters.Where(r => r.Source.Skip(1).Any())
                let resources = LoadDocument<ResourceProperty>(cluster.Source).Where(r => r != null)

                from resource in resources.Where(r => r.Context == cluster.Context)
                let type = resource.Properties.Where(p => p.Name == "@type").SelectMany(p => p.Value).Distinct()
                from property in resource.Properties.Where(p => p.Tags.Contains("@wkt") && p.Tags.Any(t => t.StartsWith("@cluster:geohash:")))

                where type.Any(t => cluster.ResourceId.StartsWith(t + "/" + property.Name + "/"))

                from propertyresource in property.Resources
                from resourcecompare in resources.Where(r => !(r.Context == resource.Context && r.ResourceId == resource.ResourceId))
                let resourcecomparetype = resourcecompare.Properties.Where(p => p.Name == "@type").SelectMany(p => p.Value)
                where propertyresource.Context == resourcecompare.Context && propertyresource.Type.Any(t => resourcecomparetype.Contains(t))

                from compareproperty in resourcecompare.Properties.Where(p => p.Tags.Contains("@wkt"))
                where propertyresource.Properties.Any(p => p.Name == compareproperty.Name)

                where property.Value.Any(v => compareproperty.Value.Any(cv => WKTIntersects(v, cv)))

                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Properties = new[] {
                        new Property {
                            Name = property.Name,
                            Resources = new[] {
                                new Resource {
                                    Context = resourcecompare.Context,
                                    ResourceId = resourcecompare.ResourceId
                                }
                            }
                        }
                    },
                    Source = new string[] { },
                    Modified = MetadataFor(cluster).Value<DateTime>("@last-modified")
                }
            );

            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties
                from inverseproperty in property.Properties.Where(p => p.Tags.Contains("@inverse"))
                from inverseresource in property.Resources.Where(r => r.ResourceId != null)

                select new Resource {
                    Context = inverseresource.Context,
                    ResourceId = inverseresource.ResourceId,
                    Properties = new[] {
                        new Property {
                            Name = inverseproperty.Name,
                            Value = property.Value,
                            Resources = new[] {
                                new Resource {
                                    Context = resource.Context,
                                    ResourceId = resource.ResourceId
                                }
                            }
                        }
                    },
                    Source = new string[] { MetadataFor(resource).Value<String>("@id") },
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
                    Properties = 
                        from property in g.SelectMany(resource => resource.Properties)
                        group property by property.Name into propertyG
                        select new Property {
                            Name = propertyG.Key,
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
