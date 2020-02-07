using System;
using System.Collections.Generic;

namespace Digitalisert.Raven
{
    public static class ResourceModelExtensions
    {
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
