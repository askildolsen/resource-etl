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
                    Status = resource.Status,
                    Tags = resource.Tags,
                    Properties = (
                        from property in resource.Properties.Where(r => !r.Name.StartsWith("@"))
                        select new Property
                        {
                            Name = property.Name,
                            Value = property.Value,
                            Resources =
                                from propertyresource in property.Resources
                                let reduceoutputs = LoadDocument<ResourcePropertyReferences>("ResourcePropertyReferences/" + propertyresource.Context + "/" + propertyresource.ResourceId).ReduceOutputs
                                let resourceoutputs = LoadDocument<ResourceProperty>(reduceoutputs)
                                select new Resource
                                {
                                    Context = propertyresource.Context,
                                    ResourceId = propertyresource.ResourceId,
                                    Type = resourceoutputs.SelectMany(r => r.Type).Distinct(),
                                    SubType = resourceoutputs.SelectMany(r => r.SubType).Distinct(),
                                    Status = resourceoutputs.SelectMany(r => r.Status).Distinct(),
                                    Tags = resourceoutputs.SelectMany(r => r.Tags).Distinct(),
                                    Modified = resourceoutputs.Select(r => r.Modified ?? DateTime.MinValue).Max(),
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
                    Status = new string[] {},
                    Tags = new string[] {},
                    Properties = new[] {
                        new Property {
                            Name = inverseproperty.Name,
                            Resources = new[] {
                                new Resource {
                                    Context = resource.Context,
                                    Type = resource.Type,
                                    SubType = resource.SubType,
                                    Status = resource.Status,
                                    Tags = resource.Tags,
                                    ResourceId = resource.ResourceId,
                                    Modified = resource.Modified,
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
                from resource in resources.Where(r => r.Properties.Any(p => p.Tags.Contains("@cluster:geohash")))
                let resourceproperties = LoadDocument<ResourceProperty>(resource.Source).Where(r => r != null)
                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = resource.ResourceId,
                    Type = new string[] {},
                    SubType = new string[] {},
                    Status = new string[] {},
                    Tags = new string[] {},
                    Properties =
                        from property in resourceproperties.SelectMany(r => r.Properties).Where(p => p.Tags.Contains("@cluster:geohash"))
                        select new Property
                        {
                            Name = property.Name,
                            Resources =
                                from derivedproperty in resource.Properties.Where(p => p.Name == property.Name)
                                from propertyresource in derivedproperty.Resources
                                let reduceoutputs = LoadDocument<ResourcePropertyReferences>("ResourcePropertyReferences/" + propertyresource.Context + "/" + propertyresource.ResourceId).ReduceOutputs
                                let resourceoutputs = LoadDocument<ResourceProperty>(reduceoutputs)

                                let comparepropertyname = property.Resources.SelectMany(r => r.Properties).Select(p => p.Name)
                                let compareproperty = resourceoutputs.SelectMany(r => r.Properties.Where(p => comparepropertyname.Contains(p.Name)))

                                where property.Value.Any(v => compareproperty.Any(cp => cp.Value.Any(cv => WKTIntersects(v, cv))))

                                select new Resource
                                {
                                    Context = propertyresource.Context,
                                    ResourceId = propertyresource.ResourceId,
                                    Type = resourceoutputs.SelectMany(r => r.Type).Distinct(),
                                    SubType = resourceoutputs.SelectMany(r => r.SubType).Distinct(),
                                    Status = resourceoutputs.SelectMany(r => r.Status).Distinct(),
                                    Tags = resourceoutputs.SelectMany(r => r.Tags).Distinct(),
                                    Modified = resourceoutputs.Select(r => r.Modified ?? DateTime.MinValue).Max(),
                                    Source = resourceoutputs.SelectMany(r => r.Source).Distinct()
                                }
                        },
                    Source = resource.Source,
                    Modified = resource.Modified ?? DateTime.MinValue
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId } into g

                let computedProperties =
                    from computedProperty in g.SelectMany(r => r.Properties).Where(p => p.Name.StartsWith("@"))
                    let compare =
                        from criteriaproperty in computedProperty.Properties
                        from criteriaresource in criteriaproperty.Resources
                        from propertycompare in g.SelectMany(r => r.Properties).Where(p => p.Name == criteriaproperty.Name)
                        from resourcecompare in propertycompare.Resources
                        where criteriaresource.Context == resourcecompare.Context
                            && criteriaresource.Type.All(type => resourcecompare.Type.Contains(type))
                            && criteriaresource.SubType.All(subtype => resourcecompare.SubType.Contains(subtype))
                            && criteriaresource.Status.All(status => resourcecompare.Status.Contains(status))
                        select resourcecompare
                    where compare.Any()
                    select new Property { Name = computedProperty.Name, Value = computedProperty.Value }

                select new Resource
                {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Type = g.SelectMany(r => r.Type).Union(computedProperties.Where(p => p.Name == "@type").SelectMany(p => p.Value)).Distinct(),
                    SubType = g.SelectMany(r => r.SubType).Union(computedProperties.Where(p => p.Name == "@subtype").SelectMany(p => p.Value)).Distinct(),
                    Status = g.SelectMany(r => r.Status).Union(computedProperties.Where(p => p.Name == "@status").SelectMany(p => p.Value)).Distinct(),
                    Tags = g.SelectMany(r => r.Tags).Union(computedProperties.Where(p => p.Name == "@tags").SelectMany(p => p.Value)).Distinct(),
                    Properties = (
                        from property in g.SelectMany(r => r.Properties).Where(r => !r.Name.StartsWith("@"))
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
                                    Type = resourceG.SelectMany(r => r.Type).Distinct(),
                                    SubType = resourceG.SelectMany(r => r.SubType).Distinct(),
                                    Status = resourceG.SelectMany(r => r.Status).Distinct(),
                                    Tags = resourceG.SelectMany(r => r.Tags).Distinct(),
                                    Modified = resourceG.Select(r => r.Modified ?? DateTime.MinValue).Max(),
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
