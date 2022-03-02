/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2022 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch.store;

public class Bucket {
  private final int index;
  private long count;

  public Bucket(int index, long initialCount) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public long getCount() {
    return count;
  }

  public void add() {
    count++;
  }

  public void add(long toBeAdded) {
    count += toBeAdded;
  }

  @Override
  public String toString() {
    return "Bucket{" + "index=" + index + ", count=" + count + '}';
  }
}
