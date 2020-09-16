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
                    Tags = resource.Tags.Union(
                        resource.Properties.SelectMany(p => p.Tags).Where(t => t == "@wkt").Take(1)
                    ),
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
                                    Type = (propertyresource.Type ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.Type)).Distinct(),
                                    SubType = (propertyresource.SubType ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.SubType)).Distinct(),
                                    Title = (propertyresource.Title ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.Title)).Distinct(),
                                    SubTitle = (propertyresource.SubTitle ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.SubTitle)).Distinct(),
                                    Code = (propertyresource.Code ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.Code)).Distinct(),
                                    Status = (propertyresource.Status ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.Status)).Distinct(),
                                    Tags = (propertyresource.Tags ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.Tags)).Distinct(),
                                    Source = (propertyresource.Source ?? new string[] { }).Union(resourceoutputs.SelectMany(r => r.Source)).Distinct()
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
                from property in resource.Properties
                from propertyresource in property.Resources.Where(r => r.ResourceId != null)

                select new Resource {
                    Context = propertyresource.Context,
                    ResourceId = propertyresource.ResourceId,
                    Type = propertyresource.Type ?? new string[] {},
                    SubType = propertyresource.SubType ?? new string[] {},
                    Title = propertyresource.Title ?? new string[] {},
                    SubTitle = propertyresource.SubTitle ?? new string[] {},
                    Code = propertyresource.Code ?? new string[] {},
                    Status = propertyresource.Status ?? new string[] {},
                    Tags = propertyresource.Tags ?? new string[] {},
                    Properties =
                        from inverseproperty in property.Properties.Where(p => p.Tags.Contains("@inverse"))
                        select new Property {
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
                        },
                    Source = new string[] { },
                    Modified = DateTime.MinValue
                }
            );

            AddMap<ResourceDerivedProperty>(resources =>
                from resource in resources
                from resourceproperty in LoadDocument<ResourceProperty>(resource.Source).Where(r => r != null)
                from property in resourceproperty.Properties.Where(p => p.Name == resource.Name)

                from compareproperty in resource.Properties
                from compareresourceproperty in LoadDocument<ResourceProperty>(compareproperty.Source).Where(r => r != null)

                where compareproperty.Name.EndsWith("+")
                    || property.Value.Any(v1 => compareresourceproperty.Properties.Where(p => p.Name == compareproperty.Name).SelectMany(p => p.Value).Any(v2 => WKTIntersects(v1, v2)))

                from derivedproperty in (
                    from ontologyresource in property.Resources
                    from ontologyproperty in ontologyresource.Properties
                    where ontologyproperty.Name == compareproperty.Name.Replace("+", "")
                        && ontologyresource.Context == compareresourceproperty.Context
                        && ontologyresource.Type.All(t => compareresourceproperty.Type.Contains(t))

                    select new {
                        fromresource = resourceproperty,
                        name = property.Name,
                        toresource = compareresourceproperty
                    }
                ).Union(
                    from ontologyproperty in property.Properties
                    from ontologyresource in ontologyproperty.Resources
                    where ontologyproperty.Name == compareproperty.Name.Replace("+", "")
                        && ontologyresource.Context == compareresourceproperty.Context
                        && ontologyresource.Type.All(t => compareresourceproperty.Type.Contains(t))

                    select new {
                        fromresource = compareresourceproperty,
                        name = compareproperty.Name.Replace("+", ""),
                        toresource = resourceproperty
                    }
                )

                select new Resource
                {
                    Context = derivedproperty.fromresource.Context,
                    ResourceId = derivedproperty.fromresource.ResourceId,
                    Type = new string[] {},
                    SubType = new string[] {},
                    Title = new string[] {},
                    SubTitle = new string[] {},
                    Code = new string[] {},
                    Status = new string[] {},
                    Tags = new string[] {},
                    Properties = new[] {
                        new Property
                        {
                            Name = derivedproperty.name,
                            Resources = new[] {
                                new Resource
                                {
                                    Context = derivedproperty.toresource.Context,
                                    ResourceId = derivedproperty.toresource.ResourceId,
                                    Type = derivedproperty.toresource.Type,
                                    SubType = derivedproperty.toresource.SubType,
                                    Title = derivedproperty.toresource.Title,
                                    SubTitle = derivedproperty.toresource.SubTitle,
                                    Code = derivedproperty.toresource.Code,
                                    Status = derivedproperty.toresource.Status,
                                    Tags = derivedproperty.toresource.Tags,
                                    Source = derivedproperty.toresource.Source
                                }
                            }
                        }
                    },
                    Source = new string[] { },
                    Modified = DateTime.MinValue
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
                            from formattedvalue in ResourceFormat(value, resource)
                            select formattedvalue
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

            Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

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
    }
}
