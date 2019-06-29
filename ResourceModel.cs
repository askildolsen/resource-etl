using System;
using System.Collections.Generic;

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
        public class MatrikkelenResource : ResourceMapped { }

        public static IEnumerable<Resource> Ontology =
            new Resource[] {
                new Resource {
                    Context = "N50Kartdata",
                    Type = new[] { "Fylke" },
                    Properties = new[] {
                        new Property {
                            Name = "Kommune"
                        }
                    }
                },
                new Resource {
                    Context = "N50Kartdata",
                    Type = new[] { "Kommune" },
                    Properties = new[] {
                        new Property {
                            Name = "Område",
                            Tags = new[] { "@cluster:geohash:4" },
                            Resources = new[] {
                                new Resource { Context = "N50Kartdata", Type = new[] { "Kommune" }, Properties = new[] { new Property { Name = "Område" } } },
                                new Resource { Context = "N50Kartdata", Type = new[] { "Naturvernområde" }, Properties = new[] { new Property { Name = "Område" } } }
                            }
                        }
                    }
                },
                new Resource {
                    Context = "N50Kartdata",
                    Type = new[] { "Naturvernområde" },
                    Properties = new[] {
                        new Property {
                            Name = "Område",
                            Tags = new[] { "@cluster:geohash:4" },
                            Resources = new[] {
                                new Resource { Context = "N50Kartdata", Type = new[] { "Kommune" }, Properties = new[] { new Property { Name = "Område" } } },
                                new Resource { Context = "N50Kartdata", Type = new[] { "Naturvernområde" }, Properties = new[] { new Property { Name = "Område" } } }
                            }
                        }
                    }
                },
                new Resource {
                    Context = "N50Kartdata",
                    Type = new[] { "Stedsnavn" },
                    Properties = new[] {
                        new Property {
                            Name = "Geometri",
                            Tags = new[] { "@cluster:geohash:5" },
                            Resources = new[] {
                                new Resource { Context = "N50Kartdata", Type = new[] { "Kommune" }, Properties = new[] { new Property { Name = "Område" } } }
                            }
                        }
                    }
                },
                /*new Resource {
                    Context = "Enheter",
                    Type = new[] { "Enhet" },
                    Properties = new[] {
                        new Property {
                            Name = "Forretningsadresse",
                            Resources = new[] {
                                new Resource { Context = "Matrikkelen", Type = new[] { "Vegadresse" }, Properties = new[] { new Property { Name = "Adresse" } }  }
                            }
                        }
                    }
                },
                new Resource {
                    Context = "Matrikkelen",
                    Type = new[] { "Vegadresse" },
                    Properties = new[] {
                        new Property {
                            Name = "Adresse",
                            Resources = new[] {
                                new Resource { Context = "Enheter", Type = new[] { "Enhet" } }
                            }
                        }
                    }
                }*/
            };
    }
}
