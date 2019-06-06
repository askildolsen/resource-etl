using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourcePropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourcePropertyIndex()
        {
            AddMapForAll<ResourceMapped>(resources =>
                from resource in resources
                let context = MetadataFor(resource).Value<String>("@collection").Replace("Resource", "")
                select new Resource
                {
                    Context = context,
                    ResourceId = resource.ResourceId,
                    Properties =
                        from property in resource.Properties.Where(p => p.Resources.Any())
                        select new Property {
                            Name = property.Name,
                            Resources =
                                from propertyresource in property.Resources
                                where propertyresource.ResourceId != null
                                select new Resource {
                                    Context = context,
                                    ResourceId = propertyresource.ResourceId
                                }
                        },
                    Source = new[] { MetadataFor(resource).Value<String>("@id")},
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            AddMapForAll<ResourceMapped>(resources =>
                from resource in resources
                let context = MetadataFor(resource).Value<String>("@collection").Replace("Resource", "")
                let resourceontology =
                    from resourcetype in resource.Type
                    let ontology = LoadDocument<ResourceOntology>("ResourceOntology/" + context + "/" + resourcetype)
                    where ontology != null
                    select ontology

                select new Resource
                {
                    Context = context,
                    ResourceId = resource.ResourceId,
                    Properties = (
                        new[] {
                            new Property { Name = "@type", Value = resource.Type }
                        }
                    ).Union(
                        new[] {
                            new Property { Name = "@ontology", Resources = resourceontology }
                        }
                    ),
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
            indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "1024"} };

            return indexDefinition;
        }
    }
}
