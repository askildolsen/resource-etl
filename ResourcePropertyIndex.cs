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
                select new Resource {
                    Context = ontology.Context,
                    ResourceId = resource.ResourceId,
                    Type = resource.Type,
                    SubType = resource.SubType,
                    Title = resource.Title,
                    SubTitle = resource.SubTitle,
                    Code = resource.Code,
                    Status = resource.Status,
                    Tags = resource.Tags,
                    Properties = (IEnumerable<Property>)Properties(
                        from ontologyproperty in ontology.Properties
                        let property = resource.Properties.Where(p => p.Name == ontologyproperty.Name)
                        select new Property {
                            Name = ontologyproperty.Name.ToString(),
                            Value = property.SelectMany(p => p.Value).Union(ontologyproperty.Value).Select(v => v.ToString()).Distinct(),
                            Tags = property.SelectMany(p => p.Tags).Union(ontologyproperty.Tags).Select(v => v.ToString()).Distinct(),
                            Resources = property.SelectMany(p => p.Resources).Union(ontologyproperty.Resources),
                            Properties = property.SelectMany(p => p.Properties).Union(ontologyproperty.Properties)
                        }, resource, ontology.Context),
                    Source = new[] { MetadataFor(resource).Value<String>("@id")},
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId } into g
                select new Resource {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Type = g.SelectMany(r => r.Type).Distinct(),
                    SubType = g.SelectMany(r => r.SubType).Distinct(),
                    Title = g.SelectMany(r => r.Title).Distinct(),
                    SubTitle = g.SelectMany(r => r.SubTitle).Distinct(),
                    Code = g.SelectMany(r => r.Code).Distinct(),
                    Status = g.SelectMany(r => r.Status).Distinct(),
                    Tags = g.SelectMany(r => r.Tags).Distinct(),
                    Properties = g.SelectMany(r => r.Properties),
                    Source = g.SelectMany(r => r.Source).Distinct(),
                    Modified = g.Select(r => r.Modified).Max()
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
