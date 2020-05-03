using System.Collections.Generic;

namespace resource_etl
{
    public class ResourceModelUtils
    {
        public static IEnumerable<dynamic> Properties(IEnumerable<dynamic> properties, dynamic resource, dynamic context)
        {
            return Digitalisert.Raven.ResourceModelExtensions.Properties(properties, resource, context);
        }

        public static string GenerateHash(string key)
        {
            return Digitalisert.Raven.ResourceModelExtensions.GenerateHash(key);
        }

        public static IEnumerable<string> WKTEncodeGeohash(string wkt)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTEncodeGeohash(wkt);
        }

        public static bool WKTIntersects(string wkt1, string wkt2)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTIntersects(wkt1, wkt2);
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
