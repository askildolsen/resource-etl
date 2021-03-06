using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceOntologyIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourceOntologyIndex()
        {
            AddMap<ResourceMapping>(resources =>
                from resource in resources
                from type in resource.Type
                from ontologyreference in LoadDocument<ResourceMappingReferences>("ResourceMappingReferences/" + resource.Context + "/" + type).ReduceOutputs
                let ontology = LoadDocument<ResourceMapping>(ontologyreference)
                where ontology != null || type.StartsWith("@")
                select new Resource
                {
                    Context = resource.Context,
                    ResourceId = type + "/" + GenerateHash(resource.ResourceId).Replace("/", "/-"),
                    Tags = ontology.Tags.Union(new[] { "@ontology" }),
                    Properties = ontology.Properties,
                    Source = new[] { MetadataFor(resource).Value<String>("@id") },
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            AddMap<ResourceMapping>(resources =>
                from resource in resources
                from type in resource.Type
                from ontologyreference in LoadDocument<ResourceMappingReferences>("ResourceMappingReferences/" + resource.Context + "/" + type).ReduceOutputs
                let ontology = LoadDocument<ResourceMapping>(ontologyreference)
                where ontology != null

                from ontologypropertyresource in (
                    from ontologyproperty in ontology.Properties.Where(p => !p.Name.StartsWith("@"))
                    where !ontology.Tags.Contains("@fetch")
                    let property = resource.Properties.Where(p => p.Name == ontologyproperty.Name)
                    from propertyresource in property.SelectMany(p => p.Resources).Where(r => !String.IsNullOrEmpty(r.ResourceId))

                    select new Resource
                    {
                        Context = propertyresource.Context ?? ontology.Context,
                        ResourceId = propertyresource.ResourceId,
                        Tags = new string[] { },
                        Properties = new Property[] { },
                        Source = new string[] { }
                    }
                ).Union(
                    from ontologyproperty in ontology.Properties.Where(p => !p.Name.StartsWith("@"))
                    from ontologyresource in ontologyproperty.Resources
                    where ontologyresource.Properties.Any(p => p.Name == "@resourceId")

                    from resourceId in 
                        from resourceIdValue in ontologyresource.Properties.Where(p => p.Name == "@resourceId").SelectMany(p => p.Value)
                        from resourceIdFormattedValue in ResourceFormat(resourceIdValue, resource)
                        select resourceIdFormattedValue

                    where ontologyresource.Tags.Contains("@push") || LoadDocument<ResourceMappingReferences>("ResourceMappingReferences/" + ontologyresource.Context + "/" + resourceId) != null

                    select new Resource
                    {
                        Context = ontologyresource.Context,
                        ResourceId = resourceId,
                        Tags = ontologyresource.Tags,
                        Properties = ontologyresource.Properties.Where(p => ontologyresource.Tags.Contains("@push") || p.Tags.Contains("@push")),
                        Source = (ontologyresource.Tags.Contains("@push") || ontologyresource.Properties.Any(p => p.Tags.Contains("@push")))
                            ? new[] { MetadataFor(resource).Value<String>("@id") }
                            : new string[] { }
                    }
                ).Union(
                    from aliasValue in ontology.Properties.Where(p => p.Name == "@alias").SelectMany(p => p.Value)
                    from aliasFormattedValue in ResourceFormat(aliasValue, resource)

                    select new Resource
                    {
                        Context = ontology.Context,
                        ResourceId = aliasFormattedValue,
                        Tags = new[] { "@alias" },
                        Properties = new Property[] { },
                        Source = new[] { MetadataFor(resource).Value<String>("@id") }
                    }
                ).Union(
                    from aliasValue in ontology.Properties.Where(p => p.Name == "@alias").SelectMany(p => p.Value)
                    from aliasFormattedValue in ResourceFormat(aliasValue, resource)

                    select new Resource
                    {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Tags = new string[] { },
                        Properties = new[] {
                            new Property {
                                Name = "@alias",
                                Source = new[] { "ResourceOntologyReferences/" + resource.Context + "/" + aliasFormattedValue }
                            }
                        },
                        Source = new string[] { }
                    }
                )

                select new Resource
                {
                    Context = ontologypropertyresource.Context,
                    ResourceId = ontologypropertyresource.ResourceId,
                    Tags = ontologypropertyresource.Tags,
                    Properties = ontologypropertyresource.Properties,
                    Source = ontologypropertyresource.Source,
                    Modified = MetadataFor(resource).Value<DateTime>("@last-modified")
                }
            );

            Reduce = results =>
                from result in results
                group result by new { result.Context, result.ResourceId } into g
                select new Resource
                {
                    Context = g.Key.Context,
                    ResourceId = g.Key.ResourceId,
                    Tags = g.SelectMany(resource => resource.Tags).Distinct(),
                    Properties = g.SelectMany(resource => resource.Properties).Distinct(),
                    Source = g.SelectMany(resource => resource.Source).Distinct(),
                    Modified = g.Select(resource => resource.Modified).Max()
                };

            Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

            OutputReduceToCollection = "ResourceOntology";
            PatternForOutputReduceToCollectionReferences = r => $"ResourceOntologyReferences/{r.Context}/{r.ResourceId}";

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
