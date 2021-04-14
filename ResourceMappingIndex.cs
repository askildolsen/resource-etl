using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using static resource_etl.ResourceModel;

namespace resource_etl
{
    public class ResourceMappingIndex : AbstractMultiMapIndexCreationTask<Resource>
    {
        public ResourceMappingIndex()
        {
            AddMapForAll<ResourceMapped>(resources =>
                from resource in resources
                select new Resource
                {
                    Context = resource.Context ?? MetadataFor(resource).Value<String>("@collection").Replace("Resource", ""),
                    ResourceId = resource.ResourceId,
                    Type = resource.Type,
                    SubType = resource.SubType,
                    Title = resource.Title,
                    SubTitle = resource.SubTitle,
                    Code = resource.Code,
                    Body = resource.Body,
                    Status = resource.Status,
                    Tags = resource.Tags,
                    Properties = resource.Properties,
                    Source = resource.Source,
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
                    Type = g.SelectMany(r => r.Type).Distinct(),
                    SubType = g.SelectMany(r => r.SubType).Distinct(),
                    Title = g.SelectMany(r => r.Title).Distinct(),
                    SubTitle = g.SelectMany(r => r.SubTitle).Distinct(),
                    Code = g.SelectMany(r => r.Code).Distinct(),
                    Body = g.SelectMany(r => r.Body).Distinct(),
                    Status = g.SelectMany(r => r.Status).Distinct(),
                    Tags = g.SelectMany(r => r.Tags).Distinct(),
                    Properties = g.SelectMany(r => r.Properties),
                    Source = g.SelectMany(r => r.Source).Distinct(),
                    Modified = g.Select(r => r.Modified).Max()
                };

            Index(Raven.Client.Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.No);

            OutputReduceToCollection = "ResourceMapping";
            PatternForOutputReduceToCollectionReferences = r => $"ResourceMappingReferences/{r.Context}/{r.ResourceId}";
        }
    }
}
