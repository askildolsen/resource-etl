using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
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
                                where resource.ResourceId != null
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
            indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "128"} };

            return indexDefinition;
        }
    }
}
