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

                from property in cluster.Properties
                from geohash in property.Value.Select(v => v.Split(new char[] { '|' }))
                from resource in property.Resources

                from propertycompare in cluster.Properties.Union(compareclusters.SelectMany(r => r.Properties))
                from geohashcompare in propertycompare.Value.Select(v => v.Split(new char[] { '|' }))
                from resourcecompare in propertycompare.Resources

                where !(resource.Context == resourcecompare.Context && resource.ResourceId == resourcecompare.ResourceId && property.Name == propertycompare.Name)
                    && (
                        // a|ab == a|abc
                        (geohash[0] == geohashcompare[0] && geohash[1].IndexOfAny(geohashcompare[1].ToCharArray()) >= 0) ||
                        // aa|ab = a|abc, aaa|ab = a|abc
                        (geohash[0].Length > geohashcompare[0].Length && geohashcompare[1].Contains(geohash[0].Substring(geohashcompare[0].Length, 1)))
                    )

                select new ResourceProperty {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Name = property.Name,
                    Properties = new[] {
                        new Property {
                            Name = propertycompare.Name + ((
                                // a|ab|b == a|abc|b
                                (geohash[0] == geohashcompare[0] && geohash[2].IndexOfAny(geohashcompare[2].ToCharArray()) >= 0) ||
                                // aa|ab|b = a|abc|b, aaa|ab|b = a|abc|b
                                (geohash[0].Length > geohashcompare[0].Length && geohashcompare[2].Contains(geohash[0].Substring(geohashcompare[0].Length, 1)))
                            ) ? "+" : ""),
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
