using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourcePropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourcePropertyIndex()
        {
            AddMap<ResourceOntology>(ontologies =>
                from ontology in ontologies.Where(r => r.Tags.Contains("@ontology"))
                from resource in LoadDocument<ResourceMapping>(ontology.Source).Where(r => r != null)
                where !ontology.Tags.Contains("@fetch") || LoadDocument<ResourceOntologyReferences>("ResourceOntologyReferences/" + resource.Context + "/" + resource.ResourceId) != null
                select new Resource {
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
                        from ontologyproperty in ontology.Properties.Where(p => !p.Name.StartsWith("@"))
                        let property = (!ontologyproperty.Tags.Contains("@derive")) ? resource.Properties.Where(p => p.Name == ontologyproperty.Name) :
                            from ontologyderivedproperty in ontologyproperty.Properties
                            from derivedproperty in resource.Properties
                            where ontologyderivedproperty.Name == derivedproperty.Name
                                && ontologyderivedproperty.Tags.All(t => derivedproperty.Tags.Contains(t))
                                && (ontologyderivedproperty.From == null || derivedproperty.From == null || ontologyderivedproperty.From >= derivedproperty.From)
                                && (ontologyderivedproperty.Thru == null || derivedproperty.Thru == null || ontologyderivedproperty.Thru <= derivedproperty.Thru)
                            select derivedproperty

                        select new Property {
                            Name = ontologyproperty.Name,
                            Value =
                                from value in property.SelectMany(p => p.Value).Union(ontologyproperty.Value)
                                from formattedvalue in ResourceFormat(value, resource, property)
                                select formattedvalue,
                            Tags = property.SelectMany(p => p.Tags).Union(ontologyproperty.Tags).Select(v => v).Distinct(),
                            Resources = (
                                from propertyresource in property.SelectMany(p => p.Resources).Where(r => !String.IsNullOrEmpty(r.ResourceId))
                                select new Resource {
                                    Context = propertyresource.Context ?? ontology.Context,
                                    ResourceId = propertyresource.ResourceId
                                }
                            ).Union(
                                from ontologyresource in ontologyproperty.Resources
                                from resourceId in
                                    from resourceIdValue in ontologyresource.Properties.Where(p => p.Name == "@resourceId").SelectMany(p => p.Value)
                                    from resourceIdFormattedValue in ResourceFormat(resourceIdValue, resource, property)
                                    where LoadDocument<ResourceMappingReferences>("ResourceMappingReferences/" + (ontologyresource.Context ?? ontology.Context) + "/" + resourceIdFormattedValue) != null
                                    select resourceIdFormattedValue
                                select new Resource {
                                    Context = ontologyresource.Context ?? ontology.Context,
                                    ResourceId = resourceId
                                }
                            ).Union(
                                from ontologyresource in ontologyproperty.Resources
                                from resourceId in
                                    from resourceIdValue in ontologyresource.Properties.Where(p => p.Name == "@resourceId").SelectMany(p => p.Value)
                                    from resourceIdFormattedValue in ResourceFormat(resourceIdValue, resource, property)
                                    where LoadDocument<ResourceMappingReferences>("ResourceMappingReferences/" + (ontologyresource.Context ?? ontology.Context) + "/" + resourceIdFormattedValue) != null
                                    select resourceIdFormattedValue
                                let aliasreference = LoadDocument<ResourceOntologyReferences>("ResourceOntologyReferences/" + (ontologyresource.Context ?? ontology.Context) + "/" + resourceId)
                                from alias in LoadDocument<ResourceOntology>(aliasreference.ReduceOutputs)
                                from aliasproperty in alias.Properties.Where(p => p.Name == "@alias")
                                from aliaspropertyreference in LoadDocument<ResourceOntologyReferences>(aliasproperty.Source)
                                from aliaspropertyresource in LoadDocument<ResourceOntology>(aliaspropertyreference.ReduceOutputs)
                                from resourcemapping in LoadDocument<ResourceMapping>(aliaspropertyresource.Source)
                                
                                select new Resource {
                                    Context = resourcemapping.Context,
                                    ResourceId = resourcemapping.ResourceId
                                }
                            ).Union(
                                from ontologyresource in ontologyproperty.Resources
                                from resourceId in
                                    from resourceIdValue in ontologyresource.Properties.Where(p => p.Name == "@resourceId").SelectMany(p => p.Value)
                                    from resourceIdFormattedValue in ResourceFormat(resourceIdValue, resource, property)
                                    where LoadDocument<ResourceMappingReferences>("ResourceMappingReferences/" + (ontologyresource.Context ?? ontology.Context) + "/" + resourceIdFormattedValue) == null
                                    select resourceIdFormattedValue
                                let aliasreference = LoadDocument<ResourceOntologyReferences>("ResourceOntologyReferences/" + (ontologyresource.Context ?? ontology.Context) + "/" + resourceId)
                                from alias in LoadDocument<ResourceOntology>(aliasreference.ReduceOutputs).Where(r => r.Tags.Contains("@alias"))
                                from resourcemapping in LoadDocument<ResourceMapping>(alias.Source)

                                select new Resource {
                                    Context = resourcemapping.Context,
                                    ResourceId = resourcemapping.ResourceId
                                }
                            ).Union(
                                ontologyproperty.Resources.Where(r => !r.Properties.Any(p => p.Name == "@resourceId"))
                            ),
                            Properties = property.SelectMany(p => p.Properties).Union(ontologyproperty.Properties)
                        }).Where(p => p.Value.Any() || p.Resources.Any()).Union(ontology.Properties.Where(p => p.Name.StartsWith("@"))),
                    Source = new[] { MetadataFor(resource).Value<String>("@id")},
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            AddMap<ResourceOntology>(ontologies =>
                from ontology in ontologies.Where(r => r.Tags.Contains("@push") || r.Properties.Any(p => p.Tags.Contains("@push")))
                from resource in LoadDocument<ResourceMapping>(ontology.Source).Where(r => r != null)
                select new Resource {
                    Context = ontology.Context,
                    ResourceId = ontology.ResourceId,
                    Type = ontology.Properties.Where(p => p.Name == "@type").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    SubType = ontology.Properties.Where(p => p.Name == "@subtype").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    Title = ontology.Properties.Where(p => p.Name == "@title").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    SubTitle = ontology.Properties.Where(p => p.Name == "@subtitle").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    Code = ontology.Properties.Where(p => p.Name == "@code").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    Status = ontology.Properties.Where(p => p.Name == "@status").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    Tags = ontology.Properties.Where(p => p.Name == "@tags").SelectMany(p => p.Value).SelectMany(v => ResourceFormat(v, resource)).Distinct(),
                    Properties = (
                        from ontologyproperty in ontology.Properties.Where(p => !p.Name.StartsWith("@"))
                        let property = (!ontologyproperty.Tags.Contains("@derive")) ? resource.Properties.Where(p => p.Name == ontologyproperty.Name) :
                            from ontologyderivedproperty in ontologyproperty.Properties
                            from derivedproperty in resource.Properties
                            where ontologyderivedproperty.Name == derivedproperty.Name
                                && ontologyderivedproperty.Tags.All(t => derivedproperty.Tags.Contains(t))
                                && (ontologyderivedproperty.From == null || derivedproperty.From == null || ontologyderivedproperty.From >= derivedproperty.From)
                                && (ontologyderivedproperty.Thru == null || derivedproperty.Thru == null || ontologyderivedproperty.Thru <= derivedproperty.Thru)
                            select derivedproperty

                        select new Property {
                            Name = ontologyproperty.Name,
                            Value =
                                from value in property.SelectMany(p => p.Value).Union(ontologyproperty.Value)
                                from formattedvalue in ResourceFormat(value, resource, property)
                                select formattedvalue,
                            Tags = property.SelectMany(p => p.Tags).Union(ontologyproperty.Tags).Select(v => v).Distinct(),
                            Resources = (
                                from propertyresource in property.SelectMany(p => p.Resources).Where(r => !String.IsNullOrEmpty(r.ResourceId))
                                select new Resource {
                                    Context = propertyresource.Context ?? ontology.Context,
                                    ResourceId = propertyresource.ResourceId
                                }
                            ).Union(
                                from ontologyresource in ontologyproperty.Resources
                                from resourceId in 
                                    from resourceIdValue in ontologyresource.Properties.Where(p => p.Name == "@resourceId").SelectMany(p => p.Value)
                                    from resourceIdFormattedValue in ResourceFormat(resourceIdValue, resource, property)
                                    select resourceIdFormattedValue
                                select new Resource {
                                    Context = ontologyresource.Context ?? ontology.Context,
                                    ResourceId = resourceId
                                }
                            ).Union(
                                ontologyproperty.Resources.Where(r => !r.Properties.Any(p => p.Name == "@resourceId"))
                            ),
                            Properties = property.SelectMany(p => p.Properties).Union(ontologyproperty.Properties),
                            Source = (ontologyproperty.Tags.Contains("@push")) ? new[] { MetadataFor(resource).Value<String>("@id") } : new string[] { },
                        }).Where(p => p.Value.Any() || p.Resources.Any()).Union(ontology.Properties.Where(p => p.Name.StartsWith("@"))),
                    Source = (ontology.Tags.Contains("@push")) ? new[] { MetadataFor(resource).Value<String>("@id")} : new string[] { },
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
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

                select new Resource {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Type = g.SelectMany(r => r.Type).Union(computedProperties.Where(p => p.Name == "@type").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    SubType = g.SelectMany(r => r.SubType).Union(computedProperties.Where(p => p.Name == "@subtype").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Title = g.SelectMany(r => r.Title).Union(computedProperties.Where(p => p.Name == "@title").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    SubTitle = g.SelectMany(r => r.SubTitle).Union(computedProperties.Where(p => p.Name == "@subtitle").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Code = g.SelectMany(r => r.Code).Union(computedProperties.Where(p => p.Name == "@code").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Status = g.SelectMany(r => r.Status).Union(computedProperties.Where(p => p.Name == "@status").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Tags = g.SelectMany(r => r.Tags).Union(computedProperties.Where(p => p.Name == "@tags").SelectMany(p => p.Value)).Select(v => v.ToString()).Distinct(),
                    Properties = (IEnumerable<Property>)Properties(g.SelectMany(r => r.Properties)),
                    Source = g.SelectMany(r => r.Source).Distinct(),
                    Modified = g.Select(r => r.Modified).Max()
                };

            Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

            OutputReduceToCollection = "ResourceProperty";
            PatternForOutputReduceToCollectionReferences = r => $"ResourcePropertyReferences/{r.Context}/{r.ResourceId}";

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
