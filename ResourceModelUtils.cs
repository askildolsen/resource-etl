using System.Collections.Generic;

namespace resource_etl
{
    public class ResourceModelUtils
    {
        public static IEnumerable<string> ResourceFormat(string value, dynamic resource)
        {
            return Digitalisert.Raven.ResourceModelExtensions.ResourceFormat(value, resource);
        }

        public static string GenerateHash(string key)
        {
            return Digitalisert.Raven.ResourceModelExtensions.GenerateHash(key);
        }

        public static IEnumerable<string> WKTEncodeGeohash(string wkt)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTEncodeGeohash(wkt);
        }

        public static string WKTDecodeGeohash(string wkt)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTDecodeGeohash(wkt);
        }

        public static string WKTEnvelope(string wkt)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTEnvelope(wkt);
        }

        public static string WKTConvexHull(string wkt)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTConvexHull(wkt);
        }

        public static bool WKTIntersects(string wkt1, string wkt2)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTIntersects(wkt1, wkt2);
        }

        public static IEnumerable<dynamic> WKTIntersectingProperty(IEnumerable<dynamic> wkts, IEnumerable<dynamic> properties)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTIntersectingProperty(wkts, properties);
        }

        public static string ReadResourceFile(string filename)
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
