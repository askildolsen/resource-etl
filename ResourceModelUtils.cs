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
    }
}
