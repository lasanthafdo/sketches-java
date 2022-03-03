/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2022 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.uddsketch.store.Bucket;
import java.util.*;

public class UniformDDSketch implements QuantileSketch<UniformDDSketch> {

  private double logGamma;
  private double multiplier;
  private double relativeAccuracy;
  private double gamma;
  private final int maxNumBuckets;
  private int numCollapses = 0;
  private SortedMap<Integer, Bucket> positiveValueStore;
  private long zeroCount = 0;
  private double minAddressableValue;

  public UniformDDSketch(int maxNumBuckets, double alphaZero) {
    this.maxNumBuckets = maxNumBuckets;
    this.positiveValueStore = new TreeMap<>();
    this.relativeAccuracy = alphaZero;
    this.gamma = calculateGamma(alphaZero);
    this.logGamma = Math.log1p(gamma - 1);
    this.multiplier = 1 / logGamma;
    this.minAddressableValue = Math.max(Math.exp((Integer.MIN_VALUE) * logGamma), Double.MIN_VALUE);
  }

  private static double calculateGamma(double alpha) {
    return (1 + alpha) / (1 - alpha);
  }

  @Override
  public void accept(double value) {
    int index = (int) Math.ceil(Math.log(value) * multiplier);
    Bucket bucket = positiveValueStore.computeIfAbsent(index, bIdx -> new Bucket(bIdx, 0));
    bucket.add();
    while (positiveValueStore.size() > maxNumBuckets) {
      uniformCollapse();
    }
  }

  private void uniformCollapse() {
    SortedMap<Integer, Bucket> collapsedStore = new TreeMap<>();
    positiveValueStore.forEach(
        (index, bucket) -> {
          int newIndex = (int) Math.ceil(index / 2.0);
          Bucket collapsedBucket =
              collapsedStore.computeIfAbsent(newIndex, bIdx -> new Bucket(bIdx, 0));
          collapsedBucket.add(bucket.getCount());
        });
    updateParamsOnCollapse(gamma * gamma);
    positiveValueStore = collapsedStore;
  }

  private void updateParamsOnCollapse(double newGamma) {
    numCollapses++;
    gamma = newGamma * newGamma;
    relativeAccuracy = (newGamma - 1) / (newGamma + 1);
    logGamma = Math.log1p(newGamma - 1);
    multiplier = 1 / logGamma;
    minAddressableValue = Math.max(Math.exp((Integer.MIN_VALUE) * logGamma), Double.MIN_VALUE);
  }

  @Override
  public void accept(double value, double count) {
    int index = (int) Math.ceil(Math.log(value) * multiplier);
    if (value < minAddressableValue) {
      zeroCount++;
    } else {
      Bucket bucket = positiveValueStore.computeIfAbsent(index, bIdx -> new Bucket(bIdx, 0));
      bucket.add((long) count);
      while (positiveValueStore.size() > maxNumBuckets) {
        uniformCollapse();
      }
    }
  }

  @Override
  public void mergeWith(UniformDDSketch other) {
    /*
    Algorithm 3 Merge(S1, S2)
    Require: S1 = {B1i }i, S2 = {B2j }j: sketches to be merged
    Ensure: Sm ← {Bmk }k: merged sketch
    INIT(Sm)
    for each {i : B1i > 0 ∨ B2i > 0} do
        Bmi ← B1i + B2i
    end for
    if Sm.size > m then
        UNIFORMCOLLAPSE(Sm)
    end if
    return Sm
     */
    Set<Integer> bucketIndexes = new HashSet<>();
    bucketIndexes.addAll(positiveValueStore.keySet());
    bucketIndexes.addAll(other.positiveValueStore.keySet());
    for (Integer bucketIndex : bucketIndexes) {
      Bucket bucket = positiveValueStore.computeIfAbsent(bucketIndex, bIdx -> new Bucket(bIdx, 0));
      Bucket otherBucket =
          other.positiveValueStore.getOrDefault(bucketIndex, new Bucket(bucketIndex, 0));
      bucket.add(otherBucket.getCount());
    }
    while (positiveValueStore.size() > maxNumBuckets) {
      uniformCollapse();
    }
  }

  @Override
  public UniformDDSketch copy() {
    return null;
  }

  @Override
  public void clear() {}

  @Override
  public double getCount() {
    long storeCount = positiveValueStore.values().stream().mapToLong(Bucket::getCount).sum();
    return zeroCount + storeCount;
  }

  @Override
  public double getSum() {
    return 0;
  }

  @Override
  public double getValueAtQuantile(double quantile) {

    long rank = (long) (quantile * (getCount() - 1));
    if (rank < zeroCount) {
      return 0;
    }

    int bid = -1;
    long counts = zeroCount;
    for (Map.Entry<Integer, Bucket> bucketEntry : positiveValueStore.entrySet()) {
      counts += bucketEntry.getValue().getCount();
      if (counts > rank) {
        bid = bucketEntry.getKey();
        break;
      }
    }

    return Math.exp(bid * logGamma) * (1 - relativeAccuracy);
  }

  @Override
  public double[] getValuesAtQuantiles(double[] quantiles) {
    return Arrays.stream(quantiles).map(this::getValueAtQuantile).toArray();
  }

  public double getRelativeAccuracy() {
    return relativeAccuracy;
  }

  public double getGamma() {
    return gamma;
  }

  public int getNumCollapses() {
    return numCollapses;
  }

  @Override
  public String toString() {
    return "UniformDDSketch{"
        + "multiplier="
        + multiplier
        + ", relativeAccuracy="
        + relativeAccuracy
        + ", gamma="
        + gamma
        + ", maxNumBuckets="
        + maxNumBuckets
        + ", numCollapses="
        + numCollapses
        + ",\n positiveValueStore="
        + positiveValueStore
        + ", zeroCount="
        + zeroCount
        + ", minAddressableValue="
        + minAddressableValue
        + '}';
  }
}
