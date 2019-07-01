using System.Collections.Generic;

namespace resource_etl
{
    public class ResourceModelUtils
    {
        public static IEnumerable<string> WKTEncodeGeohash(string wkt, int precision)
        {
            var geometry = new NetTopologySuite.IO.WKTReader().Read(wkt);

            var geohashsize = Spatial4n.Core.Util.GeohashUtils.LookupDegreesSizeForHashLen(precision);

            var shapeFactory = new NetTopologySuite.Utilities.GeometricShapeFactory();
            shapeFactory.Height = geohashsize[0];
            shapeFactory.Width = geohashsize[1];
            shapeFactory.NumPoints = 4;

            for (double y = geometry.EnvelopeInternal.MinY - geohashsize[0]; y <= geometry.EnvelopeInternal.MaxY + geohashsize[0]; y += geohashsize[0])
            {
                for (double x = geometry.EnvelopeInternal.MinX - geohashsize[1]; x <= geometry.EnvelopeInternal.MaxX + geohashsize[1]; x += geohashsize[1])
                {
                    var geohash = Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(y, x, precision);
                    var geohashdecoded = Spatial4n.Core.Util.GeohashUtils.Decode(geohash, Spatial4n.Core.Context.SpatialContext.GEO);

                    shapeFactory.Centre = new GeoAPI.Geometries.Coordinate(geohashdecoded.GetX(), geohashdecoded.GetY());

                    if (shapeFactory.CreateRectangle().Intersects(geometry))
                    {
                        yield return geohash;
                    }
                }
            }
        }

        public static bool WKTIntersects(string wkt1, string wkt2)
        {
            var wktreader = new NetTopologySuite.IO.WKTReader();
            return wktreader.Read(wkt1).Intersects(wktreader.Read(wkt2));
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
