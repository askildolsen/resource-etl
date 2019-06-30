using System;
using System.Collections.Generic;
using System.Linq;

namespace resource_etl
{
    public class ResourceModelUtils
    {
        public static IEnumerable<string> WKTEncodeGeohash(string wkt, int precision)
        {
            var geometry = new NetTopologySuite.IO.WKTReader().Read(wkt);

            var geohashsize = Spatial4n.Core.Util.GeohashUtils.DecodeBoundary(Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(geometry.Coordinate.Y, geometry.Coordinate.X, precision), Spatial4n.Core.Context.SpatialContext.GEO);

            var shapeFactory = new NetTopologySuite.Utilities.GeometricShapeFactory();
            shapeFactory.Width = geohashsize.GetWidth();
            shapeFactory.Height = geohashsize.GetHeight();
            shapeFactory.NumPoints = 4;

            for (double y = geometry.EnvelopeInternal.MinY; y <= geometry.EnvelopeInternal.MaxY; y += geohashsize.GetHeight())
            {
                for (double x = geometry.EnvelopeInternal.MinX; x <= geometry.EnvelopeInternal.MaxX; x += geohashsize.GetWidth())
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
