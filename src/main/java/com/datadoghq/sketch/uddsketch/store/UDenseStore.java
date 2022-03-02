/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch.store;

import com.datadoghq.sketch.ddsketch.Serializer;
import com.datadoghq.sketch.ddsketch.encoding.BinEncodingMode;
import com.datadoghq.sketch.ddsketch.encoding.Flag;
import com.datadoghq.sketch.ddsketch.encoding.Output;
import com.datadoghq.sketch.ddsketch.encoding.VarEncodingHelper;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.BinAcceptor;
import com.datadoghq.sketch.ddsketch.store.Store;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class UDenseStore implements Store {

  private static final int DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT = 64;
  private static final double DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO = 0.1;

  private final int arrayLengthGrowthIncrement;
  private final int arrayLengthOverhead;

  double[] counts;
  int offset;
  int minIndex;
  int maxIndex;

  UDenseStore() {
    this(DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT);
  }

  UDenseStore(int arrayLengthGrowthIncrement) {
    this(
        arrayLengthGrowthIncrement,
        (int) (arrayLengthGrowthIncrement * DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO));
  }

  UDenseStore(int arrayLengthGrowthIncrement, int arrayLengthOverhead) {
    if (arrayLengthGrowthIncrement <= 0 || arrayLengthOverhead < 0) {
      throw new IllegalArgumentException("The array growth parameters are not valid.");
    }
    this.arrayLengthGrowthIncrement = arrayLengthGrowthIncrement;
    this.arrayLengthOverhead = arrayLengthOverhead;
    this.counts = null;
    this.offset = 0;
    this.minIndex = Integer.MAX_VALUE;
    this.maxIndex = Integer.MIN_VALUE;
  }

  UDenseStore(UDenseStore store) {
    this.arrayLengthGrowthIncrement = store.arrayLengthGrowthIncrement;
    this.arrayLengthOverhead = store.arrayLengthOverhead;
    this.minIndex = store.minIndex;
    this.maxIndex = store.maxIndex;
    if (store.counts != null && !store.isEmpty()) {
      this.counts =
          Arrays.copyOfRange(
              store.counts, store.minIndex - store.offset, store.maxIndex - store.offset + 1);
      this.offset = store.minIndex;
    } else {
      // should be zero anyway, but just in case
      this.offset = store.offset;
    }
  }

  @Override
  public void add(int index) {
    final int arrayIndex = normalize(index);
    counts[arrayIndex]++;
  }

  @Override
  public void add(int index, double count) {
    if (count < 0) {
      throw new IllegalArgumentException("The count cannot be negative.");
    }
    if (count == 0) {
      return;
    }
    final int arrayIndex = normalize(index);
    counts[arrayIndex] += count;
  }

  @Override
  public void add(Bin bin) {
    if (bin.getCount() == 0) {
      return;
    }
    final int arrayIndex = normalize(bin.getIndex());
    counts[arrayIndex] += bin.getCount();
  }

  @Override
  public void clear() {
    if (null != counts) {
      Arrays.fill(counts, 0D);
    }
    maxIndex = Integer.MIN_VALUE;
    minIndex = Integer.MAX_VALUE;
    offset = 0;
  }

  /**
   * Normalize the store, if necessary, so that the counter of the specified index can be updated.
   *
   * @param index the index of the counter to be updated
   * @return the {@code counts} array index that matches the counter to be updated
   */
  abstract int normalize(int index);

  /**
   * Adjust the {@code counts}, the {@code offset}, the {@code minIndex} and the {@code maxIndex},
   * without resizing the {@code counts} array, in order to try making it fit the specified range.
   *
   * @param newMinIndex the minimum index to be stored
   * @param newMaxIndex the maximum index to be stored
   */
  abstract void adjust(int newMinIndex, int newMaxIndex);

  void extendRange(int index) {
    extendRange(index, index);
  }

  void extendRange(int newMinIndex, int newMaxIndex) {

    newMinIndex = Math.min(newMinIndex, minIndex);
    newMaxIndex = Math.max(newMaxIndex, maxIndex);

    if (isEmpty()) {

      final int initialLength = Math.toIntExact(getNewLength(newMinIndex, newMaxIndex));
      if (null == counts || initialLength >= counts.length) {
        counts = new double[initialLength];
      }
      offset = newMinIndex;
      minIndex = newMinIndex;
      maxIndex = newMaxIndex;
      adjust(newMinIndex, newMaxIndex);

    } else if (newMinIndex >= offset && newMaxIndex < (long) offset + counts.length) {

      minIndex = newMinIndex;
      maxIndex = newMaxIndex;

    } else {

      // To avoid shifting too often when nearing the capacity of the array, we may grow it before
      // we actually reach the capacity.

      final int newLength = Math.toIntExact(getNewLength(newMinIndex, newMaxIndex));
      if (newLength > counts.length) {
        counts = Arrays.copyOf(counts, newLength);
      }

      adjust(newMinIndex, newMaxIndex);
    }
  }

  void shiftCounts(int shift) {

    final int minArrayIndex = minIndex - offset;
    final int maxArrayIndex = maxIndex - offset;

    System.arraycopy(
        counts, minArrayIndex, counts, minArrayIndex + shift, maxArrayIndex - minArrayIndex + 1);

    if (shift > 0) {
      Arrays.fill(counts, minArrayIndex, minArrayIndex + shift, 0);
    } else {
      Arrays.fill(counts, maxArrayIndex + 1 + shift, maxArrayIndex + 1, 0);
    }

    offset -= shift;
  }

  void centerCounts(int newMinIndex, int newMaxIndex) {

    final int middleIndex = newMinIndex + (newMaxIndex - newMinIndex + 1) / 2;
    shiftCounts(offset + counts.length / 2 - middleIndex);

    minIndex = newMinIndex;
    maxIndex = newMaxIndex;
  }

  void resetCounts() {
    resetCounts(minIndex, maxIndex);
  }

  void resetCounts(int fromIndex, int toIndex) {
    Arrays.fill(counts, fromIndex - offset, toIndex - offset + 1, 0);
  }

  @Override
  public boolean isEmpty() {
    return maxIndex < minIndex;
  }

  @Override
  public int getMinIndex() {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return minIndex;
  }

  @Override
  public int getMaxIndex() {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    return maxIndex;
  }

  long getNewLength(int newMinIndex, int newMaxIndex) {
    final long desiredLength = (long) newMaxIndex - newMinIndex + 1;
    return ((desiredLength + arrayLengthOverhead - 1) / arrayLengthGrowthIncrement + 1)
        * arrayLengthGrowthIncrement;
  }

  @Override
  public double getTotalCount() {
    return getTotalCount(minIndex, maxIndex);
  }

  double getTotalCount(int fromIndex, int toIndex) {

    if (isEmpty()) {
      return 0;
    }

    final int fromArrayIndex = Math.max(fromIndex - offset, 0);
    final int toArrayIndex = Math.min(toIndex - offset, counts.length - 1);

    double totalCount = 0;
    for (int arrayIndex = fromArrayIndex; arrayIndex <= toArrayIndex; arrayIndex++) {
      totalCount += counts[arrayIndex];
    }

    return totalCount;
  }

  @Override
  public void forEach(BinAcceptor acceptor) {
    if (isEmpty()) {
      return;
    }
    for (int i = minIndex; i < maxIndex; i++) {
      double value = counts[i - offset];
      if (value != 0) {
        acceptor.accept(i, value);
      }
    }
    double lastCount = counts[maxIndex - offset];
    if (lastCount != 0) {
      acceptor.accept(maxIndex, lastCount);
    }
  }

  @Override
  public Stream<Bin> getAscendingStream() {
    if (isEmpty()) {
      return Stream.of();
    }
    return IntStream.rangeClosed(minIndex, maxIndex)
        .filter(index -> counts[index - offset] > 0)
        .mapToObj(index -> new Bin(index, counts[index - offset]));
  }

  @Override
  public Stream<Bin> getDescendingStream() {
    if (isEmpty()) {
      return Stream.of();
    }
    return IntStream.iterate(maxIndex, index -> index - 1)
        .limit(maxIndex - minIndex + 1)
        .filter(index -> counts[index - offset] > 0)
        .mapToObj(index -> new Bin(index, counts[index - offset]));
  }

  @Override
  public Iterator<Bin> getAscendingIterator() {

    return new Iterator<Bin>() {

      private long index = minIndex;

      @Override
      public boolean hasNext() {
        return index <= maxIndex;
      }

      @Override
      public Bin next() {
        final int nextIndex = (int) index;
        do {
          index++;
        } while (index <= maxIndex && counts[(int) index - offset] == 0);
        return new Bin(nextIndex, counts[nextIndex - offset]);
      }
    };
  }

  @Override
  public Iterator<Bin> getDescendingIterator() {

    return new Iterator<Bin>() {

      private long index = maxIndex;

      @Override
      public boolean hasNext() {
        return index >= minIndex;
      }

      @Override
      public Bin next() {
        final int nextIndex = (int) index;
        do {
          index--;
        } while (index >= minIndex && counts[(int) index - offset] == 0);
        return new Bin(nextIndex, counts[nextIndex - offset]);
      }
    };
  }

  @Override
  public void encode(Output output, Flag.Type storeFlagType) throws IOException {
    if (isEmpty()) {
      return;
    }

    long denseEncodingSize = 0;
    final long numBins = (long) maxIndex - (long) minIndex + 1;
    denseEncodingSize += VarEncodingHelper.unsignedVarLongEncodedLength(numBins);
    denseEncodingSize += VarEncodingHelper.signedVarLongEncodedLength(minIndex);
    denseEncodingSize += VarEncodingHelper.signedVarLongEncodedLength(1);

    long sparseEncodingSize = 0;
    long numNonEmptyBins = 0;

    long previousIndex = 0;
    for (int i = minIndex - offset; i <= maxIndex - offset; i++) {
      final double count = counts[i];
      final long countVarDoubleEncodedLength = VarEncodingHelper.varDoubleEncodedLength(count);
      denseEncodingSize += countVarDoubleEncodedLength;
      if (count != 0) {
        numNonEmptyBins++;
        final long index = offset + i;
        sparseEncodingSize += VarEncodingHelper.signedVarLongEncodedLength(index - previousIndex);
        sparseEncodingSize += countVarDoubleEncodedLength;
        previousIndex = index;
      }
    }

    if (denseEncodingSize <= sparseEncodingSize) {
      encodeDensely(output, storeFlagType, numBins);
    } else {
      encodeSparsely(output, storeFlagType, numNonEmptyBins);
    }
  }

  private void encodeDensely(Output output, Flag.Type storeFlagType, long numBins)
      throws IOException {
    BinEncodingMode.CONTIGUOUS_COUNTS.toFlag(storeFlagType).encode(output);
    VarEncodingHelper.encodeUnsignedVarLong(output, numBins);
    VarEncodingHelper.encodeSignedVarLong(output, minIndex);
    VarEncodingHelper.encodeSignedVarLong(output, 1);
    for (int i = minIndex - offset; i <= maxIndex - offset; i++) {
      VarEncodingHelper.encodeVarDouble(output, counts[i]);
    }
  }

  private void encodeSparsely(Output output, Flag.Type storeFlagType, long numNonEmptyBins)
      throws IOException {
    BinEncodingMode.INDEX_DELTAS_AND_COUNTS.toFlag(storeFlagType).encode(output);
    VarEncodingHelper.encodeUnsignedVarLong(output, numNonEmptyBins);
    long previousIndex = 0;
    for (int i = minIndex - offset; i <= maxIndex - offset; i++) {
      final double count = counts[i];
      if (count != 0) {
        final long index = offset + i;
        VarEncodingHelper.encodeSignedVarLong(output, index - previousIndex);
        VarEncodingHelper.encodeVarDouble(output, count);
        previousIndex = index;
      }
    }
  }

  @Override
  public int serializedSize() {
    if (!isEmpty()) {
      return Serializer.sizeOfCompactDoubleArray(2, maxIndex - minIndex + 1)
          + Serializer.signedIntFieldSize(3, minIndex);
    }
    return 0;
  }

  @Override
  public void serialize(Serializer serializer) {
    if (!isEmpty()) {
      serializer.writeCompactArray(2, counts, minIndex - offset, maxIndex - minIndex + 1);
      serializer.writeSignedInt32(3, minIndex);
    }
  }
}
