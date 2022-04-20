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
package com.facebook.presto.hive.orc;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.parquet.retry.BackOffService;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;
    private final FileFormatDataSourceStats stats;
    private static final Logger LOGGER = Logger.get(HdfsOrcDataSource.class);

    public HdfsOrcDataSource(
            OrcDataSourceId id,
            long size,
            DataSize maxMergeDistance,
            DataSize maxReadSize,
            DataSize streamBufferSize,
            boolean lazyReadSmallRanges,
            FSDataInputStream inputStream,
            FileFormatDataSourceStats stats)
    {
        super(id, size, maxMergeDistance, maxReadSize, streamBufferSize, lazyReadSmallRanges);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        BackOffService backoff = new BackOffService(3, 1000L);
        boolean flag = false;
        try {
            long readStart = System.nanoTime();
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
            stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
        }
        catch (PrestoException e) {
            // just in case there is a Presto wrapper or hook
            LOGGER.debug("Got PrestoException while reading data" + e.toString());
            throw e;
        }
        catch (IOException e) {
            LOGGER.debug("Received IO Exception, retrying with exponential backoff");
            while (backoff.shouldRetry()) {
                LOGGER.debug("Retrying with retry count :" + backoff.getNumberOfTriesLeft() + "for path " + this + "and position " + position);
                try {
                    long start = System.nanoTime();
                    inputStream.readFully(position, buffer, bufferOffset, bufferLength);
                    stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - start);
                    flag = true;
                    break;
                }
                catch (IOException ioException) {
                    LOGGER.debug("Caught IO exception when retrying");
                    backoff.errorOccurred(ioException);
                }
            }
            if (!flag) {
                throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Getting IO exception and retry failed", e);
            }
        }
        catch (Exception e) {
            String message = format("Error reading from %s at position %s", this, position);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            if (e instanceof IOException) {
                throw new PrestoException(HIVE_FILESYSTEM_ERROR, message, e);
            }
            throw new PrestoException(HIVE_UNKNOWN_ERROR, message, e);
        }
    }
}
