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

package com.lmax.collections.coalescing.ring.buffer;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Math.min;

public final class MinimumProducerWorkCoalescingRingBuffer<K, V> implements CoalescingBuffer<K, V> {

    private final HashProvider<K> hashProvider;
    private final AtomicReferenceArray<ValueHolder<K, V>> values;
    private final Queue<V> backingQueue;
    private final ValueHolder<K, V>[] writerLocalValues;

    private static final class ValueHolder<K, V>
    {
        private K key;
        private V value;
        private int lastReadValueId;
    }

    public interface HashProvider<K>
    {
        int hash(final K key, final int keySpace);
    }

    private final int mask;
    private final int capacity;


    @SuppressWarnings("unchecked")
    public MinimumProducerWorkCoalescingRingBuffer(int coalesceCapacity, int queueCapacity, HashProvider<K> hashProvider) {
        this.hashProvider = hashProvider;
        this.capacity = nextPowerOfTwo(coalesceCapacity);
        this.mask = this.capacity - 1;
        backingQueue = new ArrayBlockingQueue<V>(queueCapacity);

        this.values = new AtomicReferenceArray<ValueHolder<K, V>>(this.capacity);
        this.writerLocalValues = new ValueHolder[capacity];

        for(int i = 0; i < capacity; i++)
        {
            final ValueHolder<K, V> valueHolder = new ValueHolder<K, V>();
            values.set(i, valueHolder);
            writerLocalValues[i] = valueHolder;
        }
    }

    private int nextPowerOfTwo(int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    public long rejectionCount() {
        return 0L;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isFull() {
        return size() == capacity;
    }

    @Override
    public boolean offer(K key, V value) {
        int index = hashProvider.hash(key, mask);
        ValueHolder<K, V> valueHolder = writerLocalValues[index];
        if(key.equals(valueHolder.key) || valueHolder.key == null)
        {
            valueHolder.value = value;
            valueHolder.key = key;
        }
        else
        {
            do {
                index = ((index + 1) & mask);
                valueHolder = writerLocalValues[index];
            } while(!key.equals(valueHolder.key));

            valueHolder.value = value;
        }

        values.lazySet(index, valueHolder);
        return true;
    }

    @Override
    public boolean offer(V value) {
        return backingQueue.offer(value);
    }

    @Override
    public int poll(Collection<? super V> bucket) {
        return fill(bucket);
    }

    @Override
    public int poll(Collection<? super V> bucket, int maxItems) {

        return fill(bucket);
    }

    private int fill(Collection<? super V> bucket) {
        V queuedValue;
        int added = 0;
        while(!backingQueue.isEmpty() && (queuedValue = backingQueue.poll()) != null)
        {
            bucket.add(queuedValue);
            added++;
        }
        for(int i = 0; i < capacity; i++)
        {
            final ValueHolder<K, V> valueHolder = values.get(i);
            if(valueHolder != null && System.identityHashCode(valueHolder.value) != valueHolder.lastReadValueId)
            {
                bucket.add(valueHolder.value);
                valueHolder.lastReadValueId = System.identityHashCode(valueHolder.value);
                added++;
            }
        }
        return added;
    }

}
