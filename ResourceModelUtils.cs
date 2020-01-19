using System.Collections.Generic;

namespace resource_etl
{
    public class ResourceModelUtils
    {
        public static IEnumerable<string> WKTEncodeGeohash(string wkt, int precision)
        {
            return Digitalisert.Raven.ResourceModelExtensions.WKTEncodeGeohash(wkt, precision);
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
