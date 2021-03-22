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
                    Tags = ontology.Tags,
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
                where ontology != null && !ontology.Tags.Contains("@fetch")

                from ontologypropertyresource in (
                    from ontologyproperty in ontology.Properties.Where(p => !p.Name.StartsWith("@"))
                    let property = resource.Properties.Where(p => p.Name == ontologyproperty.Name)
                    from propertyresource in property.SelectMany(p => p.Resources).Where(r => !String.IsNullOrEmpty(r.ResourceId))

                    select new Resource
                    {
                        Context = propertyresource.Context ?? ontology.Context,
                        ResourceId = propertyresource.ResourceId
                    }
                ).Union(
                    from ontologyproperty in ontology.Properties.Where(p => !p.Name.StartsWith("@"))
                    from ontologyresource in ontologyproperty.Resources
                    where ontologyresource.Properties.Any(p => p.Name == "@resourceId")

                    from resourceId in 
                        from resourceIdValue in ontologyresource.Properties.Where(p => p.Name == "@resourceId").SelectMany(p => p.Value)
                        from resourceIdFormattedValue in ResourceFormat(resourceIdValue, resource)
                        select resourceIdFormattedValue

                    select new Resource
                    {
                        Context = ontologyresource.Context,
                        ResourceId = resourceId
                    }
                )

                select new Resource
                {
                    Context = ontologypropertyresource.Context,
                    ResourceId = ontologypropertyresource.ResourceId,
                    Tags = new string[] { },
                    Properties = new Property[] { },
                    Source = new string[] { },
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
