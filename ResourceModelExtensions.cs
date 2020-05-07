using System;
using System.Collections.Generic;

namespace Digitalisert.Raven
{
    public static class ResourceModelExtensions
    {
        public static IEnumerable<dynamic> Properties(IEnumerable<dynamic> properties, dynamic resource, dynamic context)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        public static string ResourceFormat(string value, dynamic resource)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        public static string GenerateHash(string key)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        public static IEnumerable<string> WKTEncodeGeohash(string wkt)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        public static bool WKTIntersects(string wkt1, string wkt2)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
    }
}
