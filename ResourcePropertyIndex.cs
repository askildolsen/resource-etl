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
            AddMap<ResourceOntology>(ontologies =>
                from ontology in ontologies
                from resource in LoadDocument<ResourceMapped>(ontology.Source).Where(r => r != null)
                select new Resource
                {
                    Context = ontology.Context,
                    ResourceId = resource.ResourceId,
                    Type = resource.Type,
                    Properties = (
                        from ontologyproperty in ontology.Properties
                        from property in (
                            resource.Properties.Where(p => p.Name == ontologyproperty.Name)
                        ).Union(
                            resource.Properties.Where(p => ontologyproperty.Properties.Any(op => op.Name == p.Name))
                        ).Union(
                            from p in ontologyproperty.Properties.Where(op => op.Name.StartsWith("@"))
                            from value in new[] { new { Name = "@code", Value = resource.Code } }
                            where p.Name == value.Name
                            select new Property {
                                Value = value.Value,
                                Tags = p.Tags,
                                Resources = p.Resources,
                                Properties = p.Properties
                            }
                        )
                        select new Property {
                            Name = ontologyproperty.Name,
                            Value = property.Value.Union(ontologyproperty.Value).Distinct(),
                            Tags = property.Tags.Union(ontologyproperty.Tags).Distinct(),
                            Resources = (
                                from propertyresource in property.Resources.Where(r => r.ResourceId != null)
                                select new Resource {
                                    Context = ontology.Context,
                                    ResourceId = propertyresource.ResourceId
                                }
                            ).Union(
                                from ontologyresource in ontologyproperty.Resources
                                select ontologyresource
                            ),
                            Properties = ontologyproperty.Properties
                        }
                    ).Union(
                        ontology.Properties.Where(p => p.Name.StartsWith("@"))
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
                    Type = g.SelectMany(r => r.Type).Distinct(),
                    Properties = g.SelectMany(resource => resource.Properties).Distinct(),
                    Source = g.SelectMany(resource => resource.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified).Max()
                };

            Index(r => r.Properties, FieldIndexing.No);
            Store(r => r.Properties, FieldStorage.Yes);

            OutputReduceToCollection = "ResourceProperty";
            PatternForOutputReduceToCollectionReferences = r => $"ResourcePropertyReferences/{r.Context}/{r.ResourceId}";

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
