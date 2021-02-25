package edu.usfca.cs.chat.util;

import com.sangupta.murmur.Murmur3;

import java.util.BitSet;

public class BloomFilter
{
    private BitSet bloomSet;
    private int numHashFuncs, filterSize, numElements;

    public BloomFilter(int m, int k) {
        filterSize = m;
        bloomSet = new BitSet(filterSize);
        numHashFuncs = k;
        numElements = 0;
    }

    /* Function to clear bloom filter */
    public void clearFilter() {
        bloomSet.clear();
        numElements = 0;
    }


    /* Function to get size of objects added */
    public int getnumElements() {
        return numElements;
    }

    /* Function to get hash value for data using Murmur3 */
    private long getHash(byte[] data, long seed) {
        long hash = Murmur3.hash_x86_32(data, data.length, seed);
        return hash;
    }

    /* Function to get filter indices for each hash function applied on the data */
    private int[] getDataHashedIndices(byte[] data) {
        int[] filterIndices = new int[numHashFuncs];
        long hash1 = getHash(data, 0);
        long hash2 = getHash(data, hash1);
        for (int i = 0; i < numHashFuncs; i++)
            filterIndices[i] = (int)Math.abs((hash1 + (i * hash2)) % (filterSize));
        return filterIndices;
    }

    /* Function to add element to filter */
    public void put(byte[] data) {
        int[] filterIndices = getDataHashedIndices(data);
        for (int i : filterIndices) {
            bloomSet.set(i, true);
        }
        numElements++;
    }

    /* Function to check if an object is present */
    public boolean get(byte[] data) {
        int[] filterIndices = getDataHashedIndices(data);
        for (int i : filterIndices)
            if (!bloomSet.get(i))
                return false;
        return true;
    }

    /* return false positive probability based on m (filterSize),k (numHashFuncs) and n (numElements)
    * formula from https://brilliant.org/wiki/bloom-filter/ */
    public float getFalsePositiveProb() {
        return (float) Math.pow((1.0 - Math.pow((1.0 - (1.0/filterSize)), numHashFuncs * numElements)), numHashFuncs);
    }
}
