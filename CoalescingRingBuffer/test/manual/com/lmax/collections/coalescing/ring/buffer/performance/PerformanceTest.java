/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lmax.collections.coalescing.ring.buffer.performance;

import com.lmax.collections.coalescing.ring.buffer.CoalescingBuffer;
import com.lmax.collections.coalescing.ring.buffer.CoalescingRingBuffer;
import com.lmax.collections.coalescing.ring.buffer.MarketSnapshot;
import com.lmax.collections.coalescing.ring.buffer.MinimumProducerWorkCoalescingRingBuffer;

import static com.lmax.collections.coalescing.ring.buffer.MarketSnapshot.createMarketSnapshot;

public class PerformanceTest {
    private static final long BILLION = 1000L * 1000L * 1000L;
    private static final MarketSnapshot POISON_PILL = createMarketSnapshot(-1, -1, -1);
    private static final int NUMBER_OF_INSTRUMENTS = 10;
    private static final int SECONDS = 1000;

    private final CoalescingBuffer<Long, MarketSnapshot> buffer;
	private final long numberOfUpdates;

	public PerformanceTest(CoalescingBuffer<Long, MarketSnapshot> buffer, long numberOfUpdates) {
		this.buffer = buffer;
		this.numberOfUpdates = numberOfUpdates;
	}

	public long run() throws InterruptedException {
		gc();
		System.out.println("testing " + buffer.getClass() + " with " + numberOfUpdates + " updates...");
        StopWatch stopWatch = new StopWatch();

        Producer producer = new Producer(buffer, NUMBER_OF_INSTRUMENTS, numberOfUpdates, POISON_PILL, stopWatch);
		Consumer consumer = new Consumer(buffer, NUMBER_OF_INSTRUMENTS, POISON_PILL, stopWatch);

		producer.start();
		consumer.start();

		consumer.join();

        return computeAndPrintResults(consumer, stopWatch.nanosTaken());
    }

	private void gc() throws InterruptedException {
		for (int i = 0; i < 10; i++) {
			System.gc();
            Thread.sleep(100);
		}
	}

	private long computeAndPrintResults(Consumer consumer, long nanosTaken) {
        for (int i = 0; i < consumer.latestSnapshots.length; i++) {
            System.out.println(consumer.latestSnapshots[i]);
        }

        System.out.println(String.format("\ntime %.1fs", nanosTaken / 1000000000.0));

		double compressionRatio = (1.0 * numberOfUpdates) / consumer.readCounter;
		System.out.println(String.format("compression ratio = %.1f", compressionRatio));

		double megaOpsPerSecond = (1000.0 * numberOfUpdates) / nanosTaken;
		System.out.println(String.format("mops = %.0f", megaOpsPerSecond));

        return Math.round(megaOpsPerSecond);
	}

	public static void main(String[] args) throws Exception {
        long[] results = new long[3];
        int runNumber = 1;

        do {
//            long result = run(runNumber++, 2 * BILLION, new CoalescingRingBuffer<Long, MarketSnapshot>(1 << 20));
            long result = run(runNumber++, 2 * BILLION, new MinimumProducerWorkCoalescingRingBuffer<Long, MarketSnapshot>(2 * NUMBER_OF_INSTRUMENTS, 10, new LongHashProvider()));
            update(results, result);
            Thread.sleep(5 * SECONDS);

        } while (!areAllResultsTheSame(results));
	}

    //long result = run(runNumber++, 2 * BILLION, new MinimumProducerWorkCoalescingRingBuffer<Long, MarketSnapshot>(2 * NUMBER_OF_INSTRUMENTS, 10, new LongHashProvider()));

    private static long run(int runNumber, long numberOfUpdates, final CoalescingBuffer<Long, MarketSnapshot> buffer) throws InterruptedException {
        PerformanceTest test = new PerformanceTest(buffer, numberOfUpdates);

        System.out.println("\n======================================= run " + runNumber + " =======================================\n");
        return test.run();
    }

    private static void update(long[] results, long result) {
        System.arraycopy(results, 1, results, 0, results.length - 1);
        results[results.length - 1] = result;
    }

    private static boolean areAllResultsTheSame(long[] results) {
        long oldestResult = results[0];

        for (int i = 1; i < results.length; i++) {
            long result = results[i];

            if (result != oldestResult) {
                return false;
            }
        }

        return true;
    }

    private static class LongHashProvider implements MinimumProducerWorkCoalescingRingBuffer.HashProvider<Long>
    {
        @Override
        public int hash(final Long key, final int keySpace)
        {
            return (int) (key.longValue() & 31);
        }
    }
}