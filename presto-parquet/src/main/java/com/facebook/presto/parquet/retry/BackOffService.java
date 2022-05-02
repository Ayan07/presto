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
package com.facebook.presto.parquet.retry;

import com.facebook.airlift.log.Logger;

import java.util.Random;

public class BackOffService
{
    public static int defaultRetries = 2;
    public static long defaultWaitTimeInMills = 10000;
    private int numberOfRetries;
    private int numberOfTriesLeft;

    public BackOffService(int numberOfRetries, long defaultTimeToWait)
    {
        this.numberOfRetries = numberOfRetries;
        this.numberOfTriesLeft = numberOfRetries;
        this.defaultTimeToWait = defaultTimeToWait;
        this.timeToWait = defaultTimeToWait;
    }

    private long defaultTimeToWait;

    private long timeToWait;

    private final Random random = new Random();

    private static final Logger LOGGER = Logger.get(BackOffService.class);

    public boolean shouldRetry()
    {
        return numberOfTriesLeft > 0;
    }

    public int getNumberOfTriesLeft()
    {
        return numberOfTriesLeft;
    }

    public void errorOccurred(Exception e)
    {
        numberOfTriesLeft--;
        if (!shouldRetry()) {
            LOGGER.error("**EXPONENTIAL BACKOFF RETRY FAILED**");
        }
        LOGGER.warn(e.toString());
        LOGGER.info("Waiting before next retry");
        waitUntilNextTry();
        timeToWait += random.nextInt(1000);
    }

    private void waitUntilNextTry()
    {
        try {
            Thread.sleep(timeToWait);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getTimeToWait()
    {
        return this.timeToWait;
    }

    public void doNotRetry()
    {
        numberOfTriesLeft = 0;
    }

    public void reset()
    {
        this.numberOfTriesLeft = numberOfRetries;
        this.timeToWait = defaultTimeToWait;
    }
}
