using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceDerivedPropertyIndex : AbstractMultiMapIndexCreationTask<ResourceProperty>
    {
        public ResourceDerivedPropertyIndex()
        {
            AddMap<ResourceCluster>(resources =>
                from resource in resources
                from property in resource.Properties

                let comparepropertyreferences =
                    from clusterreference in LoadDocument<ResourceClusterReferences>(property.Source)
                    from cluster in LoadDocument<ResourceCluster>(clusterreference.ReduceOutputs)
                    from comparepropertyreference in LoadDocument<ResourceClusterReferences>(cluster.Source)
                    from c in comparepropertyreference.ReduceOutputs
                    select c

                from resourcecompare in LoadDocument<ResourceCluster>(comparepropertyreferences.Distinct())
                where !(resource.Context == resourcecompare.Context && resource.ResourceId == resourcecompare.ResourceId)

                from resourcecompareproperty in resourcecompare.Properties

                where property.Value.Any(wkt1 => resourcecompareproperty.Value.Any(wkt2 => WKTIntersects(wkt1, wkt2)))

                select new ResourceProperty
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Name = property.Name,
                    Properties = new[] {
                        new Property {
                            Name = property.Name,
                            Source = resourcecompare.Source
                        }
                    },
                    Source = resource.Source
                }
            );

            AddMap<ResourceCluster>(resources =>
                from resource in resources
                from property in resource.Properties
                from inverseproperty in property.Properties

                let comparepropertyreferences =
                    from clusterreference in LoadDocument<ResourceClusterReferences>(inverseproperty.Source)
                    from cluster in LoadDocument<ResourceCluster>(clusterreference.ReduceOutputs)
                    from comparepropertyreference in LoadDocument<ResourceClusterReferences>(cluster.Source)
                    from c in comparepropertyreference.ReduceOutputs
                    select c

                from resourcecompare in LoadDocument<ResourceCluster>(comparepropertyreferences.Distinct())
                where !(resource.Context == resourcecompare.Context && resource.ResourceId == resourcecompare.ResourceId)

                from resourcecompareproperty in resourcecompare.Properties

                where property.Value.Any(wkt1 => resourcecompareproperty.Value.Any(wkt2 => WKTIntersects(wkt1, wkt2)))

                select new ResourceProperty
                {
                    Context = resourcecompare.Context,
                    ResourceId = resourcecompare.ResourceId,
                    Name = inverseproperty.Name,
                    Properties = new[] {
                        new Property {
                            Name = inverseproperty.Name,
                            Source = resource.Source
                        }
                    },
                    Source = resourcecompare.Source
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId, result.Name } into g
                select new ResourceProperty
                {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Name = g.Key.Name,
                    Properties = 
                        from property in g.SelectMany(resource => resource.Properties)
                        group property by property.Name into propertyG
                        select new Property {
                            Name = propertyG.Key,
                            Value = propertyG.SelectMany(p => p.Value).Distinct(),
                            Tags = propertyG.SelectMany(p => p.Tags).Distinct(),
                            Source = propertyG.SelectMany(p => p.Source).Distinct()
                        },
                    Source = g.SelectMany(resource => resource.Source).Distinct()
                };

            Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

            OutputReduceToCollection = "ResourceDerivedProperty";
            PatternForOutputReduceToCollectionReferences = r => $"ResourceDerivedPropertyReferences/{r.Context}/{r.ResourceId}/{r.Name}";

            Configuration["Indexing.MapBatchSize"] = "128";

            AdditionalSources = new Dictionary<string, string>
            {
                {
                    "ResourceModelUtils",
                    ReadResourceFile("resource_etl.ResourceModelUtils.cs")
                }
            };
        }
    }
}
