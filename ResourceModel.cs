using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceModel
    {
        public class Resource
        {
            public string Context { get; set; }
            public string ResourceId { get; set; }
            public IEnumerable<string> Type { get; set; }
            public IEnumerable<string> SubType { get; set; }
            public IEnumerable<string> Title { get; set; }
            public IEnumerable<string> SubTitle { get; set; }
            public IEnumerable<string> Code { get; set; }
            public IEnumerable<string> Status { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }
            public DateTime? Modified { get; set; }
        }

        public class Property
        {
            public string Name { get; set; }
            public IEnumerable<string> Value { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Resource> Resources { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }
        }

        public class ResourceCluster : Resource { }
        public class ResourceProperty : Resource { }
        public class ResourceDerivedProperty : ResourceProperty { }
        public class ResourceInverseProperty : Resource { }
        public class ResourceMapped : Resource { }
        public class EnheterResource : ResourceMapped { }
        public class N50KartdataResource : ResourceMapped { }

        public class ResourcePropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourcePropertyIndex()
            {
                AddMapForAll<ResourceMapped>(resources =>
                    from resource in resources
                    let context = MetadataFor(resource).Value<String>("@collection").Replace("Resource", "")
                    let properties =
                        from p in resource.Properties
                        select new Property {
                            Name = p.Name,
                            Resources =
                                from propertyresource in p.Resources
                                where propertyresource.ResourceId != null
                                select new Resource {
                                    Context = context,
                                    ResourceId = propertyresource.ResourceId
                                }
                        }
                    where properties.Any(p => p.Resources.Any())
                    select new Resource
                    {
                        Context = context,
                        ResourceId = resource.ResourceId,
                        Properties = properties.Where(p => p.Resources.Any()),
                        Source = new[] { MetadataFor(resource).Value<String>("@id")},
                        Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                    }
                );

                AddMapForAll<ResourceMapped>(resources =>
                    from resource in resources
                    where resource.Properties.Any(p => p.Tags.Contains("@wkt"))
                    select new Resource
                    {
                        Context = MetadataFor(resource).Value<String>("@collection").Replace("Resource", ""),
                        ResourceId = resource.ResourceId,
                        Properties = resource.Properties.Where(p => p.Tags.Contains("@wkt")),
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
                        Properties = g.SelectMany(resource => resource.Properties).Distinct(),
                        Source = g.SelectMany(resource => resource.Source).Distinct(),
                        Modified = g.Select(resource => resource.Modified).Max()
                    };

                Index(r => r.Properties, FieldIndexing.No);
                Store(r => r.Properties, FieldStorage.Yes);

                OutputReduceToCollection = "ResourceProperty";

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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "30"} };

                return indexDefinition;
            }
        }

        public class ResourceClusterIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourceClusterIndex()
            {
                AddMap<ResourceProperty>(resources =>
                    from resource in resources
                    from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                    from wkt in property.Value.Where(v => v != null)
                    from geohash in WKTEncodeGeohash(wkt, 5)
                    select new Resource
                    {
                        Context = "@geohash",
                        ResourceId = geohash,
                        Source = new[] { MetadataFor(resource).Value<String>("@id")}
                    }
                );

                Reduce = results =>
                    from result in results
                    group result by new { result.Context, result.ResourceId } into g
                    select new Resource
                    {
                        Context = g.Key.Context,
                        ResourceId = g.Key.ResourceId,
                        Source = g.SelectMany(r => r.Source).Distinct()
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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "10"} };

                return indexDefinition;
            }
        }

        public class ResourceDerivedPropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourceDerivedPropertyIndex()
            {
                AddMap<ResourceCluster>(clusters =>
                    from cluster in clusters.Where(r => r.Context == "@geohash" && r.Source.Skip(1).Any())
                    let resources = LoadDocument<ResourceProperty>(cluster.Source).Where(r => r != null)
                    from resource in resources
                    from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))

                    let intersects =
                        from resource_wkt in property.Value.Where(v => v != null)
                        from resourcecompare in resources.Where(r => !(r.Context == resource.Context && r.ResourceId == resource.ResourceId))
                        from resourcecompare_wkt in resourcecompare.Properties.Where(p => p.Tags.Contains("@wkt")).SelectMany(p => p.Value).Where(v => v != null)
                        where WKTIntersects(resource_wkt, resourcecompare_wkt)
                        select
                            new Resource {
                                Context = resourcecompare.Context,
                                ResourceId = resourcecompare.ResourceId
                            }
                    
                    where intersects.Any()

                    select new Resource
                    {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Properties = new[] {
                            new Property {
                                Name = property.Name,
                                Resources = intersects
                            }
                        },
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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "10"} };

                return indexDefinition;
            }
        }

        public class ResourceInversePropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourceInversePropertyIndex()
            {
                AddMapForAll<ResourceProperty>(resources =>
                    from resource in resources
                    select new Resource
                    {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Properties = new Property[] { },
                        Modified = resource.Modified,
                        Source = resource.Source.Union(new string[] { MetadataFor(resource).Value<String>("@id") })
                    }
                );

                AddMapForAll<ResourceProperty>(resources =>
                    from resource in resources
                    from property in resource.Properties
                    from propertyresource in property.Resources
                    where propertyresource.ResourceId != null
                    select new Resource
                    {
                        Context = propertyresource.Context,
                        ResourceId = propertyresource.ResourceId,
                        Properties = new[] {
                            new Property {
                                Name = resource.Context + "/" + property.Name,
                                Source = new[] { MetadataFor(resource).Value<String>("@id") }
                            }
                        },
                        Modified = null,
                        Source = new string[] { },
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
                                Source = propertyG.SelectMany(p => p.Source)
                            },
                        Modified = g.Select(r => r.Modified ?? DateTime.MinValue).Max(),
                        Source = g.SelectMany(resource => resource.Source).Distinct()
                    };

                Index(r => r.Properties, FieldIndexing.No);
                Store(r => r.Properties, FieldStorage.Yes);

                OutputReduceToCollection = "ResourceInverseProperty";

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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "10"} };

                return indexDefinition;
            }
        }

        public class ResourceReasonerIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourceReasonerIndex()
            {
                AddMapForAll<ResourceProperty>(resources =>
                    from resource in resources
                    select new Resource
                    {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Properties = resource.Properties,
                        Source = resource.Source,
                        Modified = resource.Modified ?? DateTime.MinValue
                    }
                );

                AddMap<ResourceInverseProperty>(resources =>
                    from resource in resources
                    from inverseproperty in resource.Properties
                    from inverseresource in LoadDocument<ResourceProperty>(inverseproperty.Source).Where(r => r != null)
                    select new Resource
                    {
                        Context = inverseresource.Context,
                        ResourceId = inverseresource.ResourceId,
                        Properties = (
                            from property in inverseresource.Properties
                            select new Property
                            {
                                Name = property.Name,
                                Resources =
                                    from propertyresource in property.Resources
                                    where propertyresource.Context == resource.Context && propertyresource.ResourceId == resource.ResourceId
                                    select new Resource
                                    {
                                        Context = propertyresource.Context,
                                        ResourceId = propertyresource.ResourceId,
                                        Properties = new[] { new Property { Source = resource.Source.Where(s => s.StartsWith("Resource")) } },
                                        Modified = resource.Modified,
                                        Source = resource.Source.Where(s => !s.StartsWith("Resource"))
                                    }
                            }
                        ).Where(p => p.Resources.Any()),
                        Source = new string[] { },
                        Modified = null
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
                            from property in g.SelectMany(r => r.Properties)
                            group property by property.Name into propertyG
                            select new Property {
                                Name = propertyG.Key,
                                Value = propertyG.SelectMany(p => p.Value).Distinct(),
                                Tags = propertyG.SelectMany(p => p.Tags).Distinct(),
                                Resources =
                                    from resource in propertyG.SelectMany(p => p.Resources)
                                    group resource by new { resource.Context, resource.ResourceId } into resourceG
                                    select new Resource {
                                        Context = resourceG.Key.Context,
                                        ResourceId = resourceG.Key.ResourceId,
                                        Properties = resourceG.SelectMany(r => r.Properties).Distinct(),
                                        Modified = resourceG.Select(r => r.Modified ?? DateTime.MinValue).Max(),
                                        Source = resourceG.SelectMany(r => r.Source).Distinct()
                                    }
                            },
                        Source = g.SelectMany(resource => resource.Source).Distinct(),
                        Modified = g.Select(resource => resource.Modified ?? DateTime.MinValue).Max()
                    };

                Index(r => r.Properties, FieldIndexing.No);
                Store(r => r.Properties, FieldStorage.Yes);

                OutputReduceToCollection = "Resource";

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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "30"} };

                return indexDefinition;
            }
        }

        private static string ReadResourceFile(string filename)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var stream = assembly.GetManifestResourceStream(filename))
            {
                using (var reader = new System.IO.StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
