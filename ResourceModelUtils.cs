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

        public static IEnumerable<dynamic> Intersects(IEnumerable<dynamic> resources)
        {
            var reader = new NetTopologySuite.IO.WKTReader();
            var intersects = new HashSet<(dynamic, dynamic, dynamic, dynamic, dynamic, dynamic)>();

            foreach(var resource in resources)
            {
                var properties = new List<dynamic>();

                foreach(var property in resource.Properties)
                {
                    var propertyresources = new List<dynamic>();

                    foreach(var value in property.Value)
                    {
                        var geometry = reader.Read(value);

                        foreach (var propertyresource in property.Resources)
                        {
                            foreach(var resourceproperty in propertyresource.Properties)
                            {
                                if (intersects.Contains((propertyresource.ResourceId, propertyresource.Context, resourceproperty.Name, resource.ResourceId, resource.Context, property.Name)))
                                {
                                    propertyresources.Add(propertyresource);
                                }
                                else {
                                    foreach(var resourcepropertyvalue in resourceproperty.Value)
                                    {
                                        var geometrycompare = reader.Read(resourcepropertyvalue);

                                        if (geometry.Intersects(geometrycompare))
                                        {
                                            intersects.Add((resource.ResourceId, resource.Context, property.Name, propertyresource.ResourceId, propertyresource.Context, resourceproperty.Name));
                                            propertyresources.Add(propertyresource);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (propertyresources.Any())
                    {
                        properties.Add(
                            new {
                                Name = property.Name,
                                Resources =
                                    from propertyresource in propertyresources
                                    select new {
                                        Context = propertyresource.Context,
                                        ResourceId = propertyresource.ResourceId
                                    }
                            }
                        );
                    }
                }

                if (properties.Any())
                {
                    yield return new {
                        Context = resource.Context,
                        ResourceId = resource.ResourceId,
                        Properties = properties
                    };
                }
            }
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
