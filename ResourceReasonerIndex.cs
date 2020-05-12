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
            AddMap<ResourceProperty>(resources =>
                from resource in resources
                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Type = resource.Type,
                    SubType = resource.SubType,
                    Title = resource.Title,
                    SubTitle = resource.SubTitle,
                    Code = resource.Code,
                    Status = resource.Status,
                    Tags = resource.Tags,
                    Properties = (
                        from property in resource.Properties.Where(r => !r.Name.StartsWith("@"))
                        select new Property
                        {
                            Name = property.Name,
                            Value = property.Value,
                            Resources =
                                from propertyresource in property.Resources.Where(r => r.ResourceId != null)
                                let reduceoutputs = LoadDocument<ResourcePropertyReferences>("ResourcePropertyReferences/" + propertyresource.Context + "/" + propertyresource.ResourceId).ReduceOutputs
                                let resourceoutputs = LoadDocument<ResourceProperty>(reduceoutputs)
                                select new Resource
                                {
                                    Context = propertyresource.Context,
                                    ResourceId = propertyresource.ResourceId,
                                    Type = resourceoutputs.SelectMany(r => r.Type).Distinct(),
                                    SubType = resourceoutputs.SelectMany(r => r.SubType).Distinct(),
                                    Title = resourceoutputs.SelectMany(r => r.Title).Distinct(),
                                    SubTitle = resourceoutputs.SelectMany(r => r.SubTitle).Distinct(),
                                    Code = resourceoutputs.SelectMany(r => r.Code).Distinct(),
                                    Status = resourceoutputs.SelectMany(r => r.Status).Distinct(),
                                    Tags = resourceoutputs.SelectMany(r => r.Tags).Distinct(),
                                    Source = resourceoutputs.SelectMany(r => r.Source).Distinct()
                                }
                        }
                    ).Union(
                        resource.Properties.Where(r => r.Name.StartsWith("@"))
                    ),
                    Source = resource.Source,
                    Modified = resource.Modified ?? DateTime.MinValue
                }
            );

            AddMap<ResourceProperty>(resources =>
                from resource in resources
                from property in resource.Properties.Where(p => p.Properties.Any(p => p.Tags.Contains("@inverse")))
                from propertyresource in property.Resources.Where(r => r.ResourceId != null)
                from inverseproperty in property.Properties.Where(p => p.Tags.Contains("@inverse"))

                select new Resource {
                    Context = propertyresource.Context,
                    ResourceId = propertyresource.ResourceId,
                    Type = new string[] {},
                    SubType = new string[] {},
                    Title = new string[] {},
                    SubTitle = new string[] {},
                    Code = new string[] {},
                    Status = new string[] {},
                    Tags = new string[] {},
                    Properties = new[] {
                        new Property {
                            Name = inverseproperty.Name,
                            Resources = new[] {
                                new Resource {
                                    Context = resource.Context,
                                    ResourceId = resource.ResourceId,
                                    Type = resource.Type,
                                    SubType = resource.SubType,
                                    Title = resource.Title,
                                    SubTitle = resource.SubTitle,
                                    Code = resource.Code,
                                    Status = resource.Status,
                                    Tags = resource.Tags,
                                    Source = resource.Source
                                }
                            }
                        }
                    },
                    Source = new string[] { },
                    Modified = DateTime.MinValue
                }
            );

            AddMap<ResourceDerivedProperty>(resources =>
                from resource in resources.Where(r => r.Properties.Any(p => p.Tags.Contains("@wkt")))
                let rp = LoadDocument<ResourceProperty>(resource.Source)
                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Type = new string[] {},
                    SubType = new string[] {},
                    Title = new string[] {},
                    SubTitle = new string[] {},
                    Code = new string[] {},
                    Status = new string[] {},
                    Tags = new string[] {},
                    Properties =
                        from property in resource.Properties.Where(p => p.Tags.Contains("@wkt"))
                        select new Property
                        {
                            Name = property.Name,
                            Resources =
                                from propertyresourcereferences in LoadDocument<ResourceDerivedPropertyReferences>(property.Source)
                                from propertyresource in LoadDocument<ResourceDerivedProperty>(propertyresourcereferences.ReduceOutputs)

                                where property.Value.Any(v => propertyresource.Properties.Any(cp => cp.Value.Any(cv => WKTIntersects(v, cv))))

                                let resourceproperty = LoadDocument<ResourceProperty>(propertyresource.Source)

                                where rp.SelectMany(r => r.Properties.Where(p => p.Name == resource.Name)).SelectMany(p => p.Value).Any(v =>
                                    resourceproperty.SelectMany(r => r.Properties.Where(p => p.Name == propertyresource.Name)).SelectMany(p => p.Value).Any(cv =>
                                        WKTIntersects(v, cv)
                                    )
                                )

                                select new Resource
                                {
                                    Context = propertyresource.Context,
                                    ResourceId = propertyresource.ResourceId,
                                    Type = resourceproperty.SelectMany(r => r.Type).Distinct(),
                                    SubType = resourceproperty.SelectMany(r => r.SubType).Distinct(),
                                    Title = resourceproperty.SelectMany(r => r.Title).Distinct(),
                                    SubTitle = resourceproperty.SelectMany(r => r.SubTitle).Distinct(),
                                    Code = resourceproperty.SelectMany(r => r.Code).Distinct(),
                                    Status = resourceproperty.SelectMany(r => r.Status).Distinct(),
                                    Tags = resourceproperty.SelectMany(r => r.Tags).Distinct(),
                                    Source = resourceproperty.SelectMany(r => r.Source).Distinct()
                                }
                        },
                    Source = new string[] { },
                    Modified = resource.Modified ?? DateTime.MinValue
                }
            );

            AddMap<ResourceCluster>(clusters =>
                from cluster in clusters.Where(r => r.Context == "@geohash")
                let resourceproperties = LoadDocument<ResourceProperty>(cluster.Source).Where(r => r != null)
                select new Resource
                {
                    Context = cluster.Context,
                    ResourceId = cluster.ResourceId,
                    Type = new string[] {},
                    SubType = new string[] {},
                    Title = new string[] {},
                    SubTitle = new string[] {},
                    Code = new string[] {},
                    Status = new string[] {},
                    Tags = new string[] {},
                    Properties = (
                        new[] {
                            new Property {
                                Name = "Geohash",
                                Value = new[] { WKTDecodeGeohash(cluster.ResourceId) },
                                Tags = new[] { "@wkt" }
                            }
                        }
                    ).Union(
                        new[] {
                            new Property {
                                Name = "References",
                                Resources =
                                    from propertyresource in resourceproperties
                                    select new Resource
                                    {
                                        Context = propertyresource.Context,
                                        ResourceId = propertyresource.ResourceId,
                                        Type = propertyresource.Type,
                                        SubType = propertyresource.SubType,
                                        Status = propertyresource.Status,
                                        Tags = propertyresource.Tags,
                                        Source = propertyresource.Source
                                    }
                            }
                        }
                    ),
                    Source = new string[] { },
                    Modified = cluster.Modified ?? DateTime.MinValue
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId } into g

                let computedProperties =
                    from property in g.SelectMany(r => r.Properties).Where(p => p.Name.StartsWith("@"))
                    select new Property {
                        Name = property.Name,
                        Value = (
                            from value in property.Value
                            from resource in g.ToList()
                            select ResourceFormat(value, resource)
                        ).Where(v => !String.IsNullOrWhiteSpace(v))
                    }

                select new Resource
                {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Type = g.SelectMany(r => r.Type).Union(computedProperties.Where(p => p.Name == "@type").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    SubType = g.SelectMany(r => r.SubType).Union(computedProperties.Where(p => p.Name == "@subtype").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Title = g.SelectMany(r => r.Title).Union(computedProperties.Where(p => p.Name == "@title").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    SubTitle = g.SelectMany(r => r.SubTitle).Union(computedProperties.Where(p => p.Name == "@subtitle").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Code = g.SelectMany(r => r.Code).Union(computedProperties.Where(p => p.Name == "@code").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Status = g.SelectMany(r => r.Status).Union(computedProperties.Where(p => p.Name == "@status").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Tags = g.SelectMany(r => r.Tags).Union(computedProperties.Where(p => p.Name == "@tags").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Properties = (
                        from property in g.SelectMany(r => r.Properties).Where(r => !r.Name.StartsWith("@"))
                        group property by property.Name into propertyG
                        select new Property {
                            Name = propertyG.Key,
                            Value = propertyG.SelectMany(p => p.Value).Distinct(),
                            Tags = propertyG.SelectMany(p => p.Tags).Distinct(),
                            Resources =
                                from resource in propertyG.SelectMany(p => p.Resources)
                                group resource by new { resource.Context, resource.ResourceId } into resourceG
                                select new Resource {
                                    Context = resourceG.Key.Context,
                                    ResourceId = resourceG.Key.ResourceId,
                                    Type = resourceG.SelectMany(r => r.Type).Distinct(),
                                    SubType = resourceG.SelectMany(r => r.SubType).Distinct(),
                                    Title = resourceG.SelectMany(r => r.Title).Distinct(),
                                    SubTitle = resourceG.SelectMany(r => r.SubTitle).Distinct(),
                                    Code = resourceG.SelectMany(r => r.Code).Distinct(),
                                    Status = resourceG.SelectMany(r => r.Status).Distinct(),
                                    Tags = resourceG.SelectMany(r => r.Tags).Distinct(),
                                    Source = resourceG.SelectMany(r => r.Source).Distinct()
                                }
                        }
                    ).Union(
                        g.SelectMany(r => r.Properties).Where(r => r.Name.StartsWith("@"))
                    ),
                    Source = g.SelectMany(resource => resource.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified ?? DateTime.MinValue).Max()
                };

            Index(r => r.Properties, FieldIndexing.No);
            Store(r => r.Properties, FieldStorage.Yes);

            OutputReduceToCollection = "Resource";
            PatternForOutputReduceToCollectionReferences = r => $"ResourceReferences/{r.Context}/{r.ResourceId}";

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
