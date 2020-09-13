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
            AddMap<ResourceCluster>(clusters =>
                from cluster in clusters
                let compareclusters =
                    from clusterreference in LoadDocument<ResourceClusterReferences>(cluster.Source).Where(r => r != null)
                    from comparecluster in LoadDocument<ResourceCluster>(clusterreference.ReduceOutputs)
                    select comparecluster

                let compareproperties = cluster.Properties.Union(compareclusters.SelectMany(r => r.Properties).ToList())
                from property in cluster.Properties

                let geohashes = property.Value.Select(v => v.ToString().Replace("+", "")).ToList()
                let convexhull = property.Properties.Where(p => p.Name == "@convexhull").SelectMany(p => p.Value)

                from resource in property.Resources

                from propertycompare in cluster.Properties.Union(compareclusters.SelectMany(r => r.Properties))
                let geohashescompare = propertycompare.Value.Select(v => v.ToString().Replace("+", "")).ToList()
                let geohashescomparecovers = propertycompare.Value.Where(v => v.EndsWith("+")).Select(v => v.ToString().Replace("+", "")).ToList()
                let convexhullcompare = propertycompare.Properties.Where(p => p.Name == "@convexhull").SelectMany(p => p.Value)
                
                from derivedproperty in (
                    from resourcecompare in propertycompare.Resources
                    where !(resource.Context == resourcecompare.Context && resource.ResourceId == resourcecompare.ResourceId && property.Name == propertycompare.Name)

                    where property.Properties.SelectMany(p => p.Resources).Where(r => r.Properties.Any(p => p.Name == propertycompare.Name)).Any(r =>
                        r.Context == resourcecompare.Context
                        && r.Type.All(t => resourcecompare.Type.Contains(t))
                    )

                    where geohashes.Any(v1 => geohashescompare.Any(v2 => v1.StartsWith(v2)))
                        && convexhull.Any(e1 => convexhullcompare.Any(e2 => WKTIntersects(e1, e2)))
                    
                    select new ResourceProperty {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Name = property.Name,
                        Properties = new[] {
                            new Property {
                                Name = propertycompare.Name
                                    + ((geohashes.Any(v1 => geohashescomparecovers.Any(v2 => v1.StartsWith(v2)))) ? "+" : ""),
                                Source = resourcecompare.Source
                            }
                        },
                        Source = resource.Source
                    }
                ).Union(
                    from resourcecompare in propertycompare.Resources
                    where !(resource.Context == resourcecompare.Context && resource.ResourceId == resourcecompare.ResourceId && property.Name == propertycompare.Name)

                    where property.Properties.SelectMany(p => p.Properties).Where(p => p.Name == propertycompare.Name).SelectMany(p => p.Resources).Any(r =>
                        r.Context == resourcecompare.Context
                        && r.Type.All(t => resourcecompare.Type.Contains(t))
                    )

                    where geohashes.Any(v1 => geohashescompare.Any(v2 => v1.StartsWith(v2)))
                        && convexhull.Any(e1 => convexhullcompare.Any(e2 => WKTIntersects(e1, e2)))
                    
                    select new ResourceProperty {
                        Context = resourcecompare.Context,
                        ResourceId = resourcecompare.ResourceId,
                        Name = propertycompare.Name,
                        Properties = new[] {
                            new Property {
                                Name = property.Name
                                    + ((geohashes.Any(v1 => geohashescomparecovers.Any(v2 => v1.StartsWith(v2)))) ? "+" : ""),
                                Source = resource.Source
                            }
                        },
                        Source = resourcecompare.Source
                    }
                )

                select new ResourceProperty {
                    Context = derivedproperty.Context,
                    ResourceId = derivedproperty.ResourceId,
                    Name = derivedproperty.Name,
                    Properties = derivedproperty.Properties,
                    Source = derivedproperty.Source
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
