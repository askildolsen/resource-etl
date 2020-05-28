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
            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                select new ResourceProperty
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Name = property.Name,
                    Properties = new[] {
                        new Property {
                            Name = property.Name,
                            Value = property.Value.Select(v => WKTEnvelope(v)),
                            Tags = property.Tags
                        }
                    },
                    Source = new[] { MetadataFor(resource).Value<String>("@id")},
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            AddMap<ResourceCluster>(clusters =>
                from cluster in clusters.Where(r => r.Context == "@geohash")
                let parentclusters =
                    from i in Enumerable.Range(1, cluster.ResourceId.Length)
                    let parenthash = cluster.ResourceId.Substring(0, cluster.ResourceId.Length - i)
                    where parenthash.Length > 0
                    select LoadDocument<ResourceClusterReferences>("ResourceClusterReferences/@geohash/" + parenthash)

                let parentclustersources = LoadDocument<ResourceCluster>(parentclusters.Where(r => r != null).SelectMany(c => c.ReduceOutputs).Distinct()).Where(r => r != null)

                let clusterresources = LoadDocument<ResourceProperty>(cluster.Source).Where(r => r != null)
                let parentclusterresources = LoadDocument<ResourceProperty>(parentclustersources.SelectMany(p => p.Source).Distinct()).Where(r => r != null)

                where clusterresources.Skip(1).Any() || parentclusterresources.Any()

                let comparisons = new[] {
                    new { resources = clusterresources, resourcescompare = clusterresources },
                    new { resources = clusterresources, resourcescompare = parentclusterresources },
                    new { resources = parentclusterresources, resourcescompare = clusterresources}
                }

                from compare in comparisons
                from resource in compare.resources

                let derivedproperties =
                    from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                    select new Property {
                        Name = property.Name,
                        Source =
                            from ontologyresource in property.Resources
                            from resourcecompare in compare.resourcescompare.Where(r => !(r.Context == resource.Context && r.ResourceId == resource.ResourceId))
                            where ontologyresource.Context == resourcecompare.Context
                                && ontologyresource.Type.All(type => resourcecompare.Type.Contains(type))
                            from resourcecompareproperty in resourcecompare.Properties.Where(p1 => ontologyresource.Properties.Any(p2 => p1.Name == p2.Name))
                            select
                                "ResourceDerivedPropertyReferences/" + resourcecompare.Context + "/" + resourcecompare.ResourceId + "/" + resourcecompareproperty.Name
                    }

                from derivedproperty in derivedproperties.Where(p => p.Source.Any())

                select new ResourceProperty {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Name = derivedproperty.Name,
                    Properties = new[] { derivedproperty },
                    Source = new string[] { MetadataFor(resource).Value<String>("@id") },
                    Modified = MetadataFor(cluster).Value<DateTime>("@last-modified")
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
                    Source = g.SelectMany(resource => resource.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified).Max()
                };

            Index(r => r.Properties, FieldIndexing.No);
            Store(r => r.Properties, FieldStorage.Yes);

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
