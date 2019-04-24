using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using static resource_etl.ResourceModelUtils;

namespace resource_etl
{
    public class ResourceModel
    {
        public class Resource
        {
            public string Context { get; set; }
            public string ResourceId { get; set; }
            public IEnumerable<string> Type { get; set; }
            public IEnumerable<string> SubType { get; set; }
            public IEnumerable<string> Title { get; set; }
            public IEnumerable<string> SubTitle { get; set; }
            public IEnumerable<string> Code { get; set; }
            public IEnumerable<string> Status { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }
            public string Target { get; set; }
            public DateTime? Modified { get; set; }
        }

        public class Property
        {
            public string Name { get; set; }
            public IEnumerable<string> Value { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<Resource> Resources { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }
        }

        public class ResourceProperty : Resource { }
        public class ResourceMapped : Resource { }
        public class EnheterResource : ResourceMapped { }

        public class ResourcePropertyIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourcePropertyIndex()
            {
                AddMapForAll<ResourceMapped>(resources =>
                    from resource in resources
                    select new Resource
                    {
                        Context = MetadataFor(resource).Value<String>("@collection").Replace("Resource", ""),
                        ResourceId = resource.ResourceId,
                        Title = resource.Title,
                        Code = resource.Code,
                        Properties =
                            from p in resource.Properties
                            where p.Resources.Any(r => r.Target != null && (r.Code == null || !r.Code.Any()))
                            select new Property {
                                Name = p.Name,
                                Resources =
                                    from propertyresource in p.Resources.Where(r => r.Target != null && (r.Code == null || !r.Code.Any()))
                                    select new Resource {
                                        Target = ResourceTarget("ResourceProperty", propertyresource.Target.Split(new[] { '/' }).First().Replace("Resource", "") + propertyresource.ResourceId)
                                    }
                            },
                        Source = new[] { MetadataFor(resource).Value<String>("@id")},
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
                        Title = g.SelectMany(resource => resource.Title),
                        Code = g.SelectMany(resource => resource.Code),
                        Properties = g.SelectMany(resource => resource.Properties),
                        Source = g.SelectMany(resource => resource.Source).Distinct(),
                        Modified = g.Select(resource => resource.Modified).Max()
                    };

                Index(r => r.Properties, FieldIndexing.No);
                Store(r => r.Properties, FieldStorage.Yes);

                OutputReduceToCollection = "ResourceProperty";

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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "30"} };

                return indexDefinition;
            }
        }

        public class ResourceReasonerIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourceReasonerIndex()
            {
                AddMapForAll<ResourceProperty>(resources =>
                    from resource in resources
                    select new Resource
                    {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Properties = 
                            from p in resource.Properties
                            select new Property {
                                Name = p.Name,
                                Resources =
                                from propertyresource in LoadDocument<ResourceProperty>(p.Resources.Select(r => r.Target))
                                select new Resource { ResourceId = propertyresource.ResourceId, Code = propertyresource.Code, Title = propertyresource.Title }
                            },
                        Source = resource.Source,
                        Modified = resource.Modified
                    }
                );

                Reduce = results =>
                    from result in results
                    group result by new { result.Context, result.ResourceId } into g
                    select new Resource
                    {
                        Context = g.Key.Context,
                        ResourceId = g.Key.ResourceId,
                        Properties = g.SelectMany(r => r.Properties),
                        Source = g.SelectMany(resource => resource.Source).Distinct(),
                        Modified = g.Select(resource => resource.Modified).Max()
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
                indexDefinition.Configuration = new IndexConfiguration { { "Indexing.MapTimeoutInSec", "30"} };

                return indexDefinition;
            }
        }

        private static string ReadResourceFile(string filename)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var stream = assembly.GetManifestResourceStream(filename))
            {
                using (var reader = new System.IO.StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
