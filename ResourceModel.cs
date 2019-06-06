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

        public class ResourceCluster : Resource { }
        public class ResourceProperty : Resource { }
        public class ResourceOntology : Resource { }
        public class ResourceDerivedProperty : ResourceProperty { }
        public class ResourceInverseProperty : Resource { }
        public class ResourceMapped : Resource { }
        public class EnheterResource : ResourceMapped { }
        public class N50KartdataResource : ResourceMapped { }

        public static IEnumerable<Resource> Ontology =
            new Resource[] {
            };
    }
}
