/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.encoding.ByteArrayInput;
import com.datadoghq.sketch.ddsketch.encoding.GrowingByteArrayOutput;
import com.datadoghq.sketch.ddsketch.encoding.Input;
import com.datadoghq.sketch.ddsketch.mapping.*;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.StoreTestCase;
import com.datadoghq.sketch.uddsketch.store.UnboundedSizeUDenseStore;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

abstract class UDDSketchTest extends QuantileSketchTest<UDDSketch> {

  abstract double relativeAccuracy();

  IndexMapping mapping() {
    return new LogarithmicMapping(relativeAccuracy());
  }

  Supplier<Store> storeSupplier() {
    return UnboundedSizeUDenseStore::new;
  }

  @Override
  public UDDSketch newSketch() {
    return new UDDSketch(mapping(), storeSupplier());
  }

  @Override
  protected void assertQuantileAccurate(
      boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {
    assertQuantileAccurate(sortedValues, quantile, actualQuantileValue, relativeAccuracy());
  }

  static void assertQuantileAccurate(
      double[] sortedValues, double quantile, double actualQuantileValue, double relativeAccuracy) {
    final double lowerQuantileValue =
        sortedValues[(int) Math.floor(quantile * (sortedValues.length - 1))];
    final double upperQuantileValue =
        sortedValues[(int) Math.ceil(quantile * (sortedValues.length - 1))];

    assertAccurate(lowerQuantileValue, upperQuantileValue, actualQuantileValue, relativeAccuracy);
  }

  @Override
  protected void assertMinAccurate(double[] sortedValues, double actualMinValue) {
    assertAccurate(sortedValues[0], actualMinValue);
  }

  @Override
  protected void assertMaxAccurate(double[] sortedValues, double actualMaxValue) {
    assertAccurate(sortedValues[sortedValues.length - 1], actualMaxValue);
  }

  @Override
  protected void assertSumAccurate(double[] sortedValues, double actualSumValue) {
    // The sum is accurate if the values that have been added to the sketch have same sign.
    if (sortedValues[0] >= 0 || sortedValues[sortedValues.length - 1] <= 0) {
      assertAccurate(Arrays.stream(sortedValues).sum(), actualSumValue);
    }
  }

  @Override
  protected void assertAverageAccurate(double[] sortedValues, double actualAverageValue) {
    // The average is accurate if the values that have been added to the sketch have same sign.
    if (sortedValues[0] >= 0 || sortedValues[sortedValues.length - 1] <= 0) {
      assertAccurate(Arrays.stream(sortedValues).average().getAsDouble(), actualAverageValue);
    }
  }

  private void assertAccurate(double minExpected, double maxExpected, double actual) {
    assertAccurate(minExpected, maxExpected, actual, relativeAccuracy());
  }

  private static void assertAccurate(
      double minExpected, double maxExpected, double actual, double relativeAccuracy) {
    final double relaxedMinExpected =
        minExpected > 0
            ? minExpected * (1 - relativeAccuracy)
            : minExpected * (1 + relativeAccuracy);
    final double relaxedMaxExpected =
        maxExpected > 0
            ? maxExpected * (1 + relativeAccuracy)
            : maxExpected * (1 - relativeAccuracy);

    if (actual < relaxedMinExpected - AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR
        || actual > relaxedMaxExpected + AccuracyTester.FLOATING_POINT_ACCEPTABLE_ERROR) {
      fail();
    }
  }

  private void assertAccurate(double expected, double actual) {
    assertAccurate(expected, expected, actual);
  }

  @Override
  protected void test(boolean merged, double[] values, UDDSketch sketch) {
    assertEncodes(merged, values, sketch);
    try {
      testProtoRoundTrip(merged, values, sketch);
    } catch (InvalidProtocolBufferException e) {
      fail(e);
    }
    testEncodeDecode(merged, values, sketch);
  }

  void testProtoRoundTrip(boolean merged, double[] values, UDDSketch sketch)
      throws InvalidProtocolBufferException {
    assertEncodes(
        merged,
        values,
        UDDSketchProtoBinding.fromProto(storeSupplier(), UDDSketchProtoBinding.toProto(sketch)));
    assertEncodes(
        merged,
        values,
        UDDSketchProtoBinding.fromProto(
            storeSupplier(),
            com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(sketch.serialize())));
  }

  void testEncodeDecode(
      boolean merged, double[] values, UDDSketch sketch, Supplier<Store> finalStoreSupplier) {
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch.encode(output, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
    final UDDSketch decoded;
    try {
      decoded = UDDSketch.decode(input, finalStoreSupplier);
      assertThat(input.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }
    assertEncodes(merged, values, decoded);
  }

  void testEncodeDecode(boolean merged, double[] values, UDDSketch sketch) {
    Arrays.stream(StoreTestCase.values())
        .filter(StoreTestCase::isLossless)
        .forEach(
            storeTestCase ->
                testEncodeDecode(merged, values, sketch, storeTestCase.storeSupplier()));
  }

  @Test
  void testIndexMappingEncodingMismatch() {
    final IndexMapping mapping1 = new QuadraticallyInterpolatedMapping(relativeAccuracy());
    final UDDSketch sketch1 = new UDDSketch(mapping1, storeSupplier());
    sketch1.accept(0.9);
    final GrowingByteArrayOutput output1 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch1.encode(output1, false);
    } catch (IOException e) {
      fail(e);
    }

    final IndexMapping mapping2 = new CubicallyInterpolatedMapping(relativeAccuracy());
    final UDDSketch sketch2 = new UDDSketch(mapping2, storeSupplier());
    final GrowingByteArrayOutput output2 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    sketch2.accept(0.8);
    try {
      sketch2.encode(output2, false);
    } catch (IOException e) {
      fail(e);
    }

    final Input input1 = ByteArrayInput.wrap(output1.backingArray(), 0, output1.numWrittenBytes());
    final UDDSketch decoded;
    try {
      decoded = UDDSketch.decode(input1, storeSupplier());
      assertThat(input1.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }
    assertThat(decoded.getIndexMapping().getClass()).isEqualTo(mapping1.getClass());

    final Input input2 = ByteArrayInput.wrap(output2.backingArray(), 0, output2.numWrittenBytes());
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> decoded.decodeAndMergeWith(input2));
  }

  @Test
  void testMissingIndexMappingEncoding() {
    final IndexMapping mapping = mapping();
    final UDDSketch sketch = new UDDSketch(mapping, storeSupplier());

    final GrowingByteArrayOutput output1 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      mapping.encode(output1);
    } catch (IOException e) {
      fail(e);
    }
    final Input input1 = ByteArrayInput.wrap(output1.backingArray(), 0, output1.numWrittenBytes());
    try {
      UDDSketch.decode(input1, storeSupplier());
      assertThat(input1.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }

    final Input input2 = ByteArrayInput.wrap(new byte[] {});
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> UDDSketch.decode(input2, storeSupplier()));

    final GrowingByteArrayOutput output3 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch.encode(output3, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input3 = ByteArrayInput.wrap(output3.backingArray(), 0, output3.numWrittenBytes());
    try {
      UDDSketch.decode(input3, storeSupplier());
      assertThat(input3.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
    }

    final GrowingByteArrayOutput output4 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch.encode(output4, true);
    } catch (IOException e) {
      fail(e);
    }
    final Input input4 = ByteArrayInput.wrap(output4.backingArray(), 0, output4.numWrittenBytes());
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> UDDSketch.decode(input4, storeSupplier()));
  }

  @Test
  void testDecodeAndMergeWith() {
    final double[] values = new double[] {0.33, -7};
    final UDDSketch sketch0 = newSketch();
    final UDDSketch sketch1 = newSketch();
    sketch0.accept(values[0]);
    sketch1.accept(values[1]);
    final GrowingByteArrayOutput output0 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    final GrowingByteArrayOutput output1 = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch0.encode(output0, false);
      sketch1.encode(output1, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input0 = ByteArrayInput.wrap(output0.backingArray(), 0, output0.numWrittenBytes());
    final Input input1 = ByteArrayInput.wrap(output1.backingArray(), 0, output1.numWrittenBytes());
    final UDDSketch decoded;
    try {
      decoded = UDDSketch.decode(input0, storeSupplier());
      decoded.decodeAndMergeWith(input1);
      assertThat(input0.hasRemaining()).isFalse();
      assertThat(input1.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }
    assertEncodes(true, values, decoded);
  }

  @Test
  void testMergingByConcatenatingEncoded() {
    final double[] values = new double[] {0.33, -7};
    final UDDSketch sketch0 = newSketch();
    final UDDSketch sketch1 = newSketch();
    sketch0.accept(values[0]);
    sketch1.accept(values[1]);
    final GrowingByteArrayOutput output = GrowingByteArrayOutput.withDefaultInitialCapacity();
    try {
      sketch0.encode(output, false);
      sketch1.encode(output, false);
    } catch (IOException e) {
      fail(e);
    }
    final Input input = ByteArrayInput.wrap(output.backingArray(), 0, output.numWrittenBytes());
    final UDDSketch decoded;
    try {
      decoded = UDDSketch.decode(input, storeSupplier());
      assertThat(input.hasRemaining()).isFalse();
    } catch (IOException e) {
      fail(e);
      return;
    }
    assertEncodes(true, values, decoded);
  }

  @ParameterizedTest
  @MethodSource("values")
  void testConversion(double[] values) {
    final double gamma = (1 + relativeAccuracy()) / (1 - relativeAccuracy());

    final double initialGamma = Math.pow(gamma, 0.3);
    final double initialRelativeAccuracy = (initialGamma - 1) / (initialGamma + 1);
    final IndexMapping initialIndexMapping =
        new BitwiseLinearlyInterpolatedMapping(initialRelativeAccuracy);
    final UDDSketch initialSketch =
        new UDDSketch(initialIndexMapping, UnboundedSizeUDenseStore::new);
    Arrays.stream(values).forEach(initialSketch);

    final double newGamma = Math.pow(gamma, 0.4); // initialGamma^2 * newGamma <= gamma
    final double newRelativeAccuracy = (newGamma - 1) / (newGamma + 1);
    final IndexMapping newIndexMapping = new LogarithmicMapping(newRelativeAccuracy);
    final UDDSketch convertedSketch =
        initialSketch.convert(newIndexMapping, UnboundedSizeUDenseStore::new);

    assertEncodes(false, values, convertedSketch);
  }

  static Stream<Arguments> values() {
    return Stream.of(
        arguments(new Object[] {new double[] {0}}),
        arguments(new Object[] {new double[] {-1}}),
        arguments(new Object[] {new double[] {-1, -1, -1}}),
        arguments(new Object[] {new double[] {-1, -1, -1, 1, 1, 1}}),
        arguments(new Object[] {new double[] {-10, -10, -10}}),
        arguments(new Object[] {new double[] {-10, -10, -10, 10, 10, 10}}),
        arguments(new Object[] {IntStream.range(0, 10000).mapToDouble(i -> -2).toArray()}),
        arguments(
            new Object[] {
              IntStream.range(0, 20000).mapToDouble(i -> i % 2 == 0 ? 2 : -2).toArray()
            }),
        arguments(new Object[] {new double[] {-10, -10, -11, -11, -11}}),
        arguments(new Object[] {new double[] {-10, -10, -11, -11, -11, 10, 10, 11, 11, 11}}),
        arguments(new Object[] {IntStream.range(0, 100).mapToDouble(i -> 0).toArray()}),
        arguments(
            new Object[] {
              DoubleStream.concat(
                      IntStream.range(0, 10).mapToDouble(i -> 0),
                      IntStream.range(-100, 100).mapToDouble(i -> i))
                  .toArray()
            }),
        arguments(
            new Object[] {
              DoubleStream.concat(
                      IntStream.range(-100, 100).mapToDouble(i -> i),
                      IntStream.range(0, 10).mapToDouble(i -> 0))
                  .toArray()
            }),
        arguments(
            new Object[] {
              DoubleStream.concat(
                      IntStream.range(-100, -1).mapToDouble(i -> i),
                      IntStream.range(1, 100).mapToDouble(i -> i))
                  .toArray()
            }),
        arguments(new Object[] {IntStream.range(-10000, 0).mapToDouble(v -> v).toArray()}),
        arguments(new Object[] {IntStream.range(-10000, 10000).mapToDouble(v -> v).toArray()}),
        arguments(new Object[] {IntStream.range(0, 10000).mapToDouble(v -> -v).toArray()}),
        arguments(new Object[] {IntStream.range(0, 20000).mapToDouble(v -> 10000 - v).toArray()}),
        arguments(new Object[] {IntStream.range(0, 100).mapToDouble(i -> -Math.exp(i)).toArray()}),
        arguments(
            new Object[] {
              DoubleStream.concat(
                      IntStream.range(0, 100).mapToDouble(i -> -Math.exp(i)),
                      IntStream.range(0, 100).mapToDouble(Math::exp))
                  .toArray()
            }),
        arguments(new Object[] {IntStream.range(0, 100).mapToDouble(i -> -Math.exp(-i)).toArray()}),
        arguments(
            new Object[] {
              DoubleStream.concat(
                      IntStream.range(0, 100).mapToDouble(i -> -Math.exp(-i)),
                      IntStream.range(0, 100).mapToDouble(i -> Math.exp(-i)))
                  .toArray()
            }));
  }

  static class DDSketchTest1 extends UDDSketchTest {

    @Override
    double relativeAccuracy() {
      return 1e-1;
    }
  }

  static class DDSketchTest2 extends UDDSketchTest {

    @Override
    double relativeAccuracy() {
      return 1e-2;
    }
  }

  static class DDSketchTest3 extends UDDSketchTest {

    @Override
    double relativeAccuracy() {
      return 1e-3;
    }
  }
}
