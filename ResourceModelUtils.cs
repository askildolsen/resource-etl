using System;
using System.Collections.Generic;

namespace resource_etl
{
    public class ResourceModelUtils
    {
        public static string ResourceTarget(string Context, string ResourceId)
        {
            return Context + "/" + CalculateXXHash64(ResourceId);
        }

        private static string CalculateXXHash64(string key)
        {
            return Sparrow.Hashing.XXHash64.Calculate(key, System.Text.Encoding.UTF8).ToString();
        }

        public static IEnumerable<string> WKTEncodeGeohash(string wkt, int precision)
        {
            var geometry = new NetTopologySuite.IO.WKTReader().Read(wkt);

            var geohashsize = Spatial4n.Core.Util.GeohashUtils.DecodeBoundary(Spatial4n.Core.Util.GeohashUtils.EncodeLatLon(geometry.InteriorPoint.Y, geometry.InteriorPoint.X, precision), Spatial4n.Core.Context.SpatialContext.GEO);

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
    }
}
