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
                let clustergeohash = cluster.ResourceId.Split(new[] { '/'} ).Last()
                let compareclusters =
                    from clusterreference in LoadDocument<ResourceClusterReferences>(cluster.Source).Where(r => r != null)
                    from comparecluster in LoadDocument<ResourceCluster>(clusterreference.ReduceOutputs)
                    select comparecluster

                from property in cluster.Properties

                let geohashes = property.Value.ToList()
                let convexhull = property.Properties.Where(p => p.Name == "@convexhull").SelectMany(p => p.Value)

                from resource in property.Resources

                from propertycompare in cluster.Properties.Union(compareclusters.SelectMany(r => r.Properties))
                let geohashescompare = propertycompare.Value.Where(v => v.StartsWith(clustergeohash) || clustergeohash.StartsWith(v)).ToList()
                where geohashescompare.Any()
                let geohashescomparecovers = propertycompare.Properties.Where(p => p.Name == "@covers").SelectMany(p => p.Value).ToList()
                let convexhullcompare = propertycompare.Properties.Where(p => p.Name == "@convexhull").SelectMany(p => p.Value)

                from resourcecompare in propertycompare.Resources
                where !(resource.Context == resourcecompare.Context && resource.ResourceId == resourcecompare.ResourceId && property.Name == propertycompare.Name)

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
