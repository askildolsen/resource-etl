using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
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
            indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapBatchSize", "512"} };

            return indexDefinition;
        }
    }
}
