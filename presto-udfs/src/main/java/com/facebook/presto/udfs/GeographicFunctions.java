/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.udfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public class GeographicFunctions
{
    private GeographicFunctions() {}

    @ScalarFunction("geohash")
    @Description("geoHash. params: lat,lng, precision")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geohash_encode(@SqlType(StandardTypes.DOUBLE) double lat, @SqlType(StandardTypes.DOUBLE)
            double lng, @SqlType(StandardTypes.INTEGER) long precision)
    {
        return Slices.utf8Slice(GeoHash.encodeHash(lat, lng, (int) precision));
    }

    @ScalarFunction("decodegeohash")
    @Description("geoHash. params: geohash")
    @SqlType("array(double)")
    public static Block geohash_decode(@SqlType(StandardTypes.VARCHAR) Slice geohash)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 2);
        LatLong coordinates = GeoHash.decodeHash(geohash.toStringUtf8());
        DOUBLE.writeDouble(blockBuilder, coordinates.getLat());
        DOUBLE.writeDouble(blockBuilder, coordinates.getLon());
        return blockBuilder.build();
    }

    @Description("geoHash. params: geohash")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double geohash_decode_lat(@SqlType(StandardTypes.VARCHAR) Slice geohash)
    {
        return GeoHash.decodeHash(geohash.toStringUtf8()).getLat();
    }

    @Description("geoHash. params: geohash")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double geohash_decode_lng(@SqlType(StandardTypes.VARCHAR) Slice geohash)
    {
        return GeoHash.decodeHash(geohash.toStringUtf8()).getLon();
    }

    @Description("haversine distance. params: lat1, lng1, lat2, lng2")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double haversine(@SqlType(StandardTypes.DOUBLE) double lat1, @SqlType(StandardTypes.DOUBLE) double
            lng1, @SqlType(StandardTypes.DOUBLE) double lat2, @SqlType(StandardTypes.DOUBLE) double lng2)
    {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2.0D) * Math.sin(dLat / 2.0D) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math
                .toRadians(lat2)) * Math.sin(dLng / 2.0D) * Math.sin(dLng / 2.0D);
        double c = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a));
        return 6371.0D * c;
    }
}
