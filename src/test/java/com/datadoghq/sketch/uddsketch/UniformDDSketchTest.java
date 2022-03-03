/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch;

import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.util.accuracy.AccuracyTester;
import java.util.Arrays;
import org.apache.commons.math3.util.FastMath;

class UniformDDSketchTest extends QuantileSketchTest<UniformDDSketch> {

  private double relativeAccuracy() {
    return 0.01;
  }

  @Override
  protected UniformDDSketch newSketch() {
    int paramK = 12;
    double alphaZero = Math.tanh(FastMath.atanh(0.01) / Math.pow(2.0, paramK - 1));
    System.out.println("Alpha zero:" + alphaZero);
    return new UniformDDSketch(1024, alphaZero);
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
}
