using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
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
                let ontology = resource.Type.Select(t => LoadDocument<ResourceOntology>("ResourceOntology/" + context + "/" + t)).Where(r => r != null)
                select new Resource
                {
                    Context = context,
                    ResourceId = resource.ResourceId,
                    Properties = (
                        from ontologyproperty in ontology.SelectMany(r => r.Properties)
                        from property in (
                            resource.Properties.Where(p => p.Name == ontologyproperty.Name)
                        ).Union(
                            resource.Properties.Where(p => ontologyproperty.Properties.Any(op => op.Name == p.Name))
                        )
                        select new Property {
                            Name = ontologyproperty.Name,
                            Value = property.Value.Union(ontologyproperty.Value).Distinct(),
                            Tags = property.Tags.Union(ontologyproperty.Tags).Distinct(),
                            Resources = (
                                from propertyresource in property.Resources.Where(r => r.ResourceId != null)
                                select new Resource {
                                    Context = context,
                                    ResourceId = propertyresource.ResourceId
                                }
                            ).Union(
                                from ontologyresource in ontologyproperty.Resources
                                select ontologyresource
                            ),
                            Properties = ontologyproperty.Properties
                        }
                    ).Union(
                        ontology.SelectMany(r => r.Properties).Where(p => p.Name.StartsWith("@"))
                    ).Union(
                        new[] {
                            new Property { Name = "@type", Value = resource.Type }
                        }
                    ),
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
