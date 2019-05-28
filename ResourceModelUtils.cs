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

        public static GeoAPI.Geometries.IGeometry WKTToGeometry(string wkt)
        {
            return new NetTopologySuite.IO.WKTReader().Read(wkt);
        }

        public static IEnumerable<dynamic> Intersects(dynamic resource, IEnumerable<dynamic> comparisons)
        {
            foreach(var compare in ((IEnumerable<dynamic>)comparisons).Where(r => r.ResourceId != resource.ResourceId || r.Context != resource.Context))
            {
                if (resource.Geometry.Intersects(compare.Geometry))
                {
                    yield return new {
                        Context = compare.Context,
                        ResourceId = compare.ResourceId
                    };
                }
            }
        }
    }
}
