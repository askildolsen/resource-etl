using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceClusterIndex : AbstractMultiMapIndexCreationTask<ResourceProperty>
    {
        public ResourceClusterIndex()
        {
            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                from wkt in property.Value.Where(v => v != null)
                from geohash in WKTEncodeGeohash(wkt)
                from type in resource.Type
                select new ResourceProperty
                {
                    Context = resource.Context,
                    ResourceId = type + "/@" + geohash,
                    Name = property.Name,
                    Properties = new Property[] { },
                    Source = new[] { "ResourceClusterReferences/" + resource.Context + "/" + resource.ResourceId + "/" + property.Name } 
                }
            );

            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                let clusterreferences =
                    from wkt in property.Value.Where(v => v != null)
                    from geohash in WKTEncodeGeohash(wkt)
                    from i in Enumerable.Range(0, geohash.Length)
                    from ontologyresource in property.Resources
                    from ontologyresourceproperty in ontologyresource.Properties
                    from type in ontologyresource.Type
                    select "ResourceClusterReferences/" + ontologyresource.Context + "/" + type + "/@" + geohash.Substring(0, geohash.Length - i) + "/" + ontologyresourceproperty.Name
                select new ResourceProperty
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Name = property.Name,
                    Properties = new[] {
                        new Property {
                            Name = property.Name,
                            Value = property.Value,
                            Properties =
                                from inverseontologyresourceproperty in property.Properties.Where(p => p.Tags.Contains("@inverse"))
                                let inverseclusterreferences =
                                    from wkt in property.Value.Where(v => v != null)
                                    from geohash in WKTEncodeGeohash(wkt)
                                    from i in Enumerable.Range(0, geohash.Length)
                                    from inverseontologyresource in inverseontologyresourceproperty.Resources
                                    from inversetype in inverseontologyresource.Type
                                    select "ResourceClusterReferences/" + inverseontologyresource.Context + "/" + inversetype + "/@" + geohash.Substring(0, geohash.Length - i) + "/" + inverseontologyresourceproperty.Name
                                select new Property {
                                    Name = inverseontologyresourceproperty.Name,
                                    Source = inverseclusterreferences.Distinct()
                                },
                            Source = clusterreferences.Distinct()
                        }
                    },
                    Source = new[] { MetadataFor(resource).Value<String>("@id")}
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
                    Properties = g.SelectMany(r => r.Properties),
                    Source = g.SelectMany(r => r.Source).Distinct()
                };

            Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

            OutputReduceToCollection = "ResourceCluster";
            PatternForOutputReduceToCollectionReferences = r => $"ResourceClusterReferences/{r.Context}/{r.ResourceId}/{r.Name}";

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
