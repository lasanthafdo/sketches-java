/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch;

import static com.datadoghq.sketch.ddsketch.Serializer.doubleFieldSize;
import static com.datadoghq.sketch.ddsketch.Serializer.embeddedFieldSize;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.datadoghq.sketch.ddsketch.Serializer;
import com.datadoghq.sketch.ddsketch.encoding.*;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMappingConverter;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.uddsketch.store.UnboundedSizeUDenseStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A {@link QuantileSketch} with relative-error guarantees. This sketch computes quantile values
 * with an approximation error that is relative to the actual quantile value. It works with both
 * positive and negative input values.
 *
 * <p>For instance, using {@code DDSketch} with a relative accuracy guarantee set to 1%, if the
 * expected quantile value is 100, the computed quantile value is guaranteed to be between 99 and
 * 101. If the expected quantile value is 1000, the computed quantile value is guaranteed to be
 * between 990 and 1010.
 *
 * <p>{@code DDSketch} works by mapping floating-point input values to bins and counting the number
 * of values for each bin. The mapping to bins is handled by {@link IndexMapping}, while the
 * underlying structure that keeps track of bin counts is {@link Store}. The {@link IndexMapping}
 * and the {@link Store} supplier are provided when constructing the sketch and can be adjusted
 * based on use-case requirements. See {@link DDSketches} for preset sketches and some explanation
 * about the involved tradeoffs.
 *
 * <p>Note that negative values are inverted before being mapped to the store. That means that if
 * you use a store that collapses the lowest (resp., the highest) indexes, it will affect the input
 * values that are the closest to (resp., the farthest away from) zero.
 *
 * <p>Note that this implementation is not thread-safe.
 */
public class UDDSketch implements QuantileSketch<UDDSketch> {

  private final IndexMapping indexMapping;
  private final double minIndexedValue;
  private final double maxIndexedValue;

  private final Store negativeValueStore;
  private final Store positiveValueStore;
  private double zeroCount;

  private UDDSketch(
      IndexMapping indexMapping,
      Store negativeValueStore,
      Store positiveValueStore,
      double zeroCount,
      double minIndexedValue) {
    this.indexMapping = indexMapping;
    this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
    this.maxIndexedValue = indexMapping.maxIndexableValue();
    this.negativeValueStore = negativeValueStore;
    this.positiveValueStore = positiveValueStore;
    this.zeroCount = zeroCount;
  }

  UDDSketch(
      IndexMapping indexMapping,
      Store negativeValueStore,
      Store positiveValueStore,
      double zeroCount) {
    this(indexMapping, negativeValueStore, positiveValueStore, zeroCount, 0);
  }

  /**
   * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and
   * {@link Store} supplier.
   *
   * @param indexMapping the mapping between floating-point values and integer indices to be used by
   *     the sketch
   * @param storeSupplier the store constructor for keeping track of added values
   * @see DDSketches
   */
  public UDDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
    this(indexMapping, storeSupplier, storeSupplier, 0);
  }

  /**
   * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and
   * {@link Store} suppliers.
   *
   * @param indexMapping the mapping between floating-point values and integer indices to be used by
   *     the sketch
   * @param negativeValueStoreSupplier the store constructor for keeping track of added negative
   *     values
   * @param positiveValueStoreSupplier the store constructor for keeping track of added positive
   *     values
   * @see DDSketches
   */
  public UDDSketch(
      IndexMapping indexMapping,
      Supplier<Store> negativeValueStoreSupplier,
      Supplier<Store> positiveValueStoreSupplier) {
    this(indexMapping, negativeValueStoreSupplier.get(), positiveValueStoreSupplier.get(), 0);
  }

  /**
   * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and
   * {@link Store} supplier.
   *
   * @param indexMapping the mapping between floating-point values and integer indices to be used by
   *     the sketch
   * @param negativeValueStoreSupplier the store constructor for keeping track of added negative
   *     values
   * @param positiveValueStoreSupplier the store constructor for keeping track of added positive
   *     values
   * @param minIndexedValue the least value that should be distinguished from zero
   * @see DDSketches
   */
  public UDDSketch(
      IndexMapping indexMapping,
      Supplier<Store> negativeValueStoreSupplier,
      Supplier<Store> positiveValueStoreSupplier,
      double minIndexedValue) {
    this(
        indexMapping,
        negativeValueStoreSupplier.get(),
        positiveValueStoreSupplier.get(),
        0,
        minIndexedValue);
  }

  /**
   * Constructs a simple instance of {@code DDSketch} with the provided relative accuracy guarantee.
   *
   * <p>This is a convenience constructor, mostly for testing purposes. For preset sketches that are
   * well-suited to a production setting and to situations where performance and memory usage
   * constraints are involved, see {@link DDSketches}.
   *
   * @param relativeAccuracy the relative accuracy guarantee of the constructed sketch
   * @see DDSketches
   */
  public UDDSketch(double relativeAccuracy) {
    this(new LogarithmicMapping(relativeAccuracy), UnboundedSizeUDenseStore::new);
  }

  private UDDSketch(UDDSketch sketch) {
    this.indexMapping = sketch.indexMapping;
    this.minIndexedValue = sketch.minIndexedValue;
    this.maxIndexedValue = sketch.maxIndexedValue;
    this.negativeValueStore = sketch.negativeValueStore.copy();
    this.positiveValueStore = sketch.positiveValueStore.copy();
    this.zeroCount = sketch.zeroCount;
  }

  /**
   * Constructs an instance of {@code DDSketch} from a mapping, possibly non-empty stores and a
   * non-negative count for the zero bucket.
   *
   * @param indexMapping the index mapping
   * @param negativeValueStore the store for negative values, possibly non-empty
   * @param positiveValueStore the store for positive values, possibly non-empty
   * @param zeroCount the count for the zero bucket, which should be non-negative (and non-NaN)
   * @return a new instance of {@code DDSketch}
   * @throws IllegalArgumentException if the {@code zeroCount} is negative or NaN
   * @throws NullPointerException if one of the arguments is null
   */
  public static UDDSketch of(
      IndexMapping indexMapping,
      Store negativeValueStore,
      Store positiveValueStore,
      double zeroCount) {
    if (!(zeroCount >= 0)) {
      throw new IllegalArgumentException("The count cannot be negative.");
    }
    return new UDDSketch(
        Objects.requireNonNull(indexMapping),
        Objects.requireNonNull(negativeValueStore),
        Objects.requireNonNull(positiveValueStore),
        zeroCount);
  }

  public IndexMapping getIndexMapping() {
    return indexMapping;
  }

  public Store getNegativeValueStore() {
    return negativeValueStore;
  }

  public Store getPositiveValueStore() {
    return positiveValueStore;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if the value is outside the range that is tracked by the
   *     sketch
   */
  @Override
  public void accept(double value) {

    checkValueTrackable(value);

    if (value > minIndexedValue) {
      positiveValueStore.add(indexMapping.index(value));
    } else if (value < -minIndexedValue) {
      negativeValueStore.add(indexMapping.index(-value));
    } else {
      zeroCount++;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if the value is outside the range that is tracked by the
   *     sketch
   */
  @Override
  public void accept(double value, double count) {

    checkValueTrackable(value);

    if (count < 0) {
      throw new IllegalArgumentException("The count cannot be negative.");
    }

    if (value > minIndexedValue) {
      positiveValueStore.add(indexMapping.index(value), count);
    } else if (value < -minIndexedValue) {
      negativeValueStore.add(indexMapping.index(-value), count);
    } else {
      zeroCount += count;
    }
  }

  private void checkValueTrackable(double value) {
    if (value < -maxIndexedValue || value > maxIndexedValue) {
      throw new IllegalArgumentException(
          "The input value is outside the range that is tracked by the sketch.");
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if the other sketch does not use the same index mapping
   */
  @Override
  public void mergeWith(UDDSketch other) {
    checkMergeability(indexMapping, other.indexMapping);
    negativeValueStore.mergeWith(other.negativeValueStore);
    positiveValueStore.mergeWith(other.positiveValueStore);
    zeroCount += other.zeroCount;
  }

  private static void checkMergeability(IndexMapping indexMapping1, IndexMapping indexMapping2)
      throws IllegalArgumentException {
    if (!indexMapping1.equals(indexMapping2)) {
      throw new IllegalArgumentException(
          "The sketches are not mergeable because they do not use the same index mappings.");
    }
  }

  @Override
  public UDDSketch copy() {
    return new UDDSketch(this);
  }

  @Override
  public boolean isEmpty() {
    return zeroCount == 0 && negativeValueStore.isEmpty() && positiveValueStore.isEmpty();
  }

  @Override
  public void clear() {
    negativeValueStore.clear();
    positiveValueStore.clear();
    zeroCount = 0D;
  }

  @Override
  public double getCount() {
    return zeroCount + negativeValueStore.getTotalCount() + positiveValueStore.getTotalCount();
  }

  /**
   * Returns an approximation of the sum of the values that have been added to the sketch. If the
   * values that have been added to the sketch all have the same sign, the approximation error has
   * the relative accuracy guarantees of the {@link IndexMapping} used for this sketch.
   *
   * @return an approximation of the sum of the values that have been added to the sketch
   */
  @Override
  public double getSum() {
    final double[] sum = {0D};
    negativeValueStore.forEach((index, count) -> sum[0] -= indexMapping.value(index) * count);
    positiveValueStore.forEach((index, count) -> sum[0] += indexMapping.value(index) * count);
    return sum[0];
  }

  @Override
  public double getMinValue() {
    if (!negativeValueStore.isEmpty()) {
      return -indexMapping.value(negativeValueStore.getMaxIndex());
    } else if (zeroCount > 0) {
      return 0;
    } else {
      return indexMapping.value(positiveValueStore.getMinIndex());
    }
  }

  @Override
  public double getMaxValue() {
    if (!positiveValueStore.isEmpty()) {
      return indexMapping.value(positiveValueStore.getMaxIndex());
    } else if (zeroCount > 0) {
      return 0;
    } else {
      return -indexMapping.value(negativeValueStore.getMinIndex());
    }
  }

  @Override
  public double getValueAtQuantile(double quantile) {
    return getValueAtQuantile(quantile, getCount());
  }

  @Override
  public double[] getValuesAtQuantiles(double[] quantiles) {
    final double count = getCount();
    return Arrays.stream(quantiles).map(quantile -> getValueAtQuantile(quantile, count)).toArray();
  }

  private double getValueAtQuantile(double quantile, double count) {

    if (quantile < 0 || quantile > 1) {
      throw new IllegalArgumentException("The quantile must be between 0 and 1.");
    }

    if (count == 0) {
      throw new NoSuchElementException();
    }

    final double rank = quantile * (count - 1);

    double n = 0;

    Iterator<Bin> negativeBinIterator = negativeValueStore.getDescendingIterator();
    while (negativeBinIterator.hasNext()) {
      Bin bin = negativeBinIterator.next();
      if ((n += bin.getCount()) > rank) {
        return -indexMapping.value(bin.getIndex());
      }
    }

    if ((n += zeroCount) > rank) {
      return 0;
    }

    Iterator<Bin> positiveBinIterator = positiveValueStore.getAscendingIterator();
    while (positiveBinIterator.hasNext()) {
      Bin bin = positiveBinIterator.next();
      if ((n += bin.getCount()) > rank) {
        return indexMapping.value(bin.getIndex());
      }
    }

    throw new NoSuchElementException();
  }

  /**
   * Builds a new {@code DDSketch} that encodes the content of this sketch with the specified index
   * mapping and the specified stores. This {@code DDSketch} is not modified by the operation.
   *
   * <p>Note that this conversion degrades the accuracy of the new sketch to the extent that it is
   * not necessarily upper-bounded by the accuracy of the provided index mapping. See {@link
   * IndexMappingConverter#distributingUniformly(IndexMapping, IndexMapping)} for details.
   *
   * @param newIndexMapping the index mapping that the new sketch should use to map values to bins
   * @param storeSupplier a constructor of an initially empty store to be used to encode bins
   * @return a new {@code DDSketch}
   */
  public UDDSketch convert(IndexMapping newIndexMapping, Supplier<Store> storeSupplier) {
    final IndexMappingConverter indexMappingConverter =
        IndexMappingConverter.distributingUniformly(indexMapping, newIndexMapping);

    final Store newNegativeValueStore = storeSupplier.get();
    indexMappingConverter.convertAscendingIterator(
        negativeValueStore.getAscendingIterator(), newNegativeValueStore::add);

    final Store newPositiveValueStore = storeSupplier.get();
    indexMappingConverter.convertAscendingIterator(
        positiveValueStore.getAscendingIterator(), newPositiveValueStore::add);

    return new UDDSketch(
        newIndexMapping, newNegativeValueStore, newPositiveValueStore, zeroCount, minIndexedValue);
  }

  public void encode(Output output, boolean omitIndexMapping) throws IOException {
    if (!omitIndexMapping) {
      indexMapping.encode(output);
    }

    if (zeroCount != 0) {
      Flag.ZERO_COUNT.encode(output);
      VarEncodingHelper.encodeVarDouble(output, zeroCount);
    }

    positiveValueStore.encode(output, Flag.Type.POSITIVE_STORE);
    negativeValueStore.encode(output, Flag.Type.NEGATIVE_STORE);
  }

  public void decodeAndMergeWith(Input input) throws IOException {
    decodeAndMergeWith(input, UDDSketch::ignoreExactSummaryStatisticFlags);
  }

  void decodeAndMergeWith(Input input, Decoder fallback) throws IOException {
    final DecodingState state =
        new DecodingState(indexMapping, negativeValueStore, positiveValueStore, zeroCount);
    decodeAndMergeWith(state, input, fallback);
    zeroCount = state.zeroCount;
  }

  public static UDDSketch decode(Input input, Supplier<Store> storeSupplier) throws IOException {
    return decode(input, storeSupplier, null);
  }

  public static UDDSketch decode(
      Input input, Supplier<Store> storeSupplier, IndexMapping indexMapping) throws IOException {
    return decode(input, storeSupplier, indexMapping, UDDSketch::ignoreExactSummaryStatisticFlags);
  }

  public static UDDSketch decode(
      Input input, Supplier<Store> storeSupplier, IndexMapping indexMapping, Decoder fallback)
      throws IOException {
    final DecodingState state =
        new DecodingState(indexMapping, storeSupplier.get(), storeSupplier.get(), 0);
    decodeAndMergeWith(state, input, fallback);
    if (state.indexMapping == null) {
      throw new IllegalArgumentException("The index mapping is missing.");
    }
    return new UDDSketch(
        state.indexMapping, state.negativeValueStore, state.positiveValueStore, state.zeroCount);
  }

  private static void decodeAndMergeWith(DecodingState state, Input input, Decoder fallback)
      throws IOException {
    while (input.hasRemaining()) {
      final Flag flag = Flag.decode(input);
      switch (flag.type()) {
        case POSITIVE_STORE:
          state.positiveValueStore.decodeAndMergeWith(input, BinEncodingMode.ofFlag(flag));
          break;
        case NEGATIVE_STORE:
          state.negativeValueStore.decodeAndMergeWith(input, BinEncodingMode.ofFlag(flag));
          break;
        case INDEX_MAPPING:
          final IndexMapping decodedIndexMapping =
              IndexMapping.decode(input, IndexMappingLayout.ofFlag(flag));
          if (state.indexMapping == null) {
            state.indexMapping = decodedIndexMapping;
          } else {
            checkMergeability(state.indexMapping, decodedIndexMapping);
          }
          break;
        case SKETCH_FEATURES:
          if (Flag.ZERO_COUNT.equals(flag)) {
            state.zeroCount += VarEncodingHelper.decodeVarDouble(input);
          } else {
            fallback.decode(input, flag);
          }
          break;
        default:
          throw new MalformedInputException("The flag type is invalid.");
      }
    }
  }

  static void ignoreExactSummaryStatisticFlags(Input input, Flag flag) throws IOException {
    if (Flag.COUNT.equals(flag)) {
      VarEncodingHelper.decodeVarDouble(input);
    } else if (Flag.SUM.equals(flag) || Flag.MIN.equals(flag) || Flag.MAX.equals(flag)) {
      input.readDoubleLE();
    } else {
      UDDSketch.throwInvalidFlagException(input, flag);
    }
  }

  static void throwInvalidFlagException(Input input, Flag flag) throws IOException {
    throw new MalformedInputException("The flag is invalid.");
  }

  interface Decoder {
    void decode(Input input, Flag flag) throws IOException;
  }

  private static final class DecodingState {
    private IndexMapping indexMapping;
    private final Store negativeValueStore;
    private final Store positiveValueStore;
    private double zeroCount;

    private DecodingState(
        IndexMapping indexMapping,
        Store negativeValueStore,
        Store positiveValueStore,
        double zeroCount) {
      this.indexMapping = indexMapping;
      this.negativeValueStore = negativeValueStore;
      this.positiveValueStore = positiveValueStore;
      this.zeroCount = zeroCount;
    }
  }

  /** @return the size of the sketch when serialized in protobuf */
  public int serializedSize() {
    return embeddedFieldSize(1, indexMapping.serializedSize())
        + embeddedFieldSize(2, positiveValueStore.serializedSize())
        + embeddedFieldSize(3, negativeValueStore.serializedSize())
        + doubleFieldSize(4, zeroCount);
  }

  /**
   * Produces protobuf encoded bytes which are equivalent to using the official protobuf bindings,
   * without requiring a runtime dependency on protobuf-java.
   *
   * <p>Currently this API is asymmetric in that there is not an equivalent method to deserialize a
   * sketch from a protobuf message. {@code DDSketchProtoBinding} can be used for this purpose, but
   * requires a runtime dependency on protobuf-java. This API may be made symmetric in the future.
   *
   * @return the sketch serialized as a {@code ByteBuffer}.
   */
  public ByteBuffer serialize() {
    int indexMappingSize = indexMapping.serializedSize();
    int positiveValueStoreSize = positiveValueStore.serializedSize();
    int negativeValueStoreSize = negativeValueStore.serializedSize();
    int totalSize =
        embeddedFieldSize(1, indexMappingSize)
            + embeddedFieldSize(2, positiveValueStoreSize)
            + embeddedFieldSize(3, negativeValueStoreSize)
            + doubleFieldSize(4, zeroCount);
    Serializer serializer = new Serializer(totalSize);
    serializer.writeHeader(1, indexMappingSize);
    indexMapping.serialize(serializer);
    serializer.writeHeader(2, positiveValueStoreSize);
    positiveValueStore.serialize(serializer);
    serializer.writeHeader(3, negativeValueStoreSize);
    negativeValueStore.serialize(serializer);
    serializer.writeDouble(4, zeroCount);
    return serializer.getBuffer();
  }

  double getZeroCount() {
    return zeroCount;
  }
}
