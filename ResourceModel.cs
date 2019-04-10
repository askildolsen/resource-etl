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
        }

        public class Property
        {
            public string Name { get; set; }
            public IEnumerable<string> Value { get; set; }
            public IEnumerable<string> Tags { get; set; }
            public IEnumerable<ResourceModel.Property.Resource> Resources { get; set; }
            public IEnumerable<Property> Properties { get; set; }
            public IEnumerable<string> Source { get; set; }

            public class Resource : ResourceModel.Resource {
                public string Target { get; set; }
            }
        }

        public class EnheterResource : Resource { }

        public class ResourceReasonerIndex : AbstractMultiMapIndexCreationTask<Resource>
        {
            public ResourceReasonerIndex()
            {
                AddMap<EnheterResource>(enheter =>
                    from enhet in enheter
                    select new Resource
                    {
                        Context = "Enheter",
                        ResourceId = enhet.ResourceId,
                        Properties =
                            from p in enhet.Properties.Where(pr => pr.Resources.Any(r => r.Target != null && !(r.Code ?? new string[] { }).Any()))
                            let targets =
                                from resourcetarget in p.Resources.Where(r => r.Target != null && (r.Code == null || !r.Code.Any()))
                                select resourcetarget.Target
                            where targets.Any()
                            select new Property {
                                Name = p.Name,
                                Resources =
                                    from resource in LoadDocument<EnheterResource>(targets)
                                    select new Property.Resource { ResourceId = resource.ResourceId, Code = resource.Code, Title = resource.Title }
                            },
                        Source = new string[] { MetadataFor(enhet).Value<String>("@id") }
                    }
                );

                Reduce = results  =>
                    from result in results
                    group result by new { result.Context, result.ResourceId } into g
                    select new Resource
                    {
                        Context = g.Key.Context,
                        ResourceId = g.Key.ResourceId,
                        Properties = g.SelectMany(r => r.Properties),
                        Source = g.SelectMany(resource => resource.Source).Distinct()
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
