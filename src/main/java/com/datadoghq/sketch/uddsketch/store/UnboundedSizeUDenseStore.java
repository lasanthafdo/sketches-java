/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2021 Datadog, Inc.
 */

package com.datadoghq.sketch.uddsketch.store;

import com.datadoghq.sketch.ddsketch.store.Store;

public class UnboundedSizeUDenseStore extends UDenseStore {

  public UnboundedSizeUDenseStore() {
    super();
  }

  public UnboundedSizeUDenseStore(int arrayLengthGrowthIncrement) {
    super(arrayLengthGrowthIncrement);
  }

  public UnboundedSizeUDenseStore(int arrayLengthGrowthIncrement, int arrayLengthOverhead) {
    super(arrayLengthGrowthIncrement, arrayLengthOverhead);
  }

  private UnboundedSizeUDenseStore(UnboundedSizeUDenseStore store) {
    super(store);
  }

  @Override
  int normalize(int index) {

    if (index < minIndex || index > maxIndex) {
      extendRange(index);
    }

    return index - offset;
  }

  @Override
  void adjust(int newMinIndex, int newMaxIndex) {
    centerCounts(newMinIndex, newMaxIndex);
  }

  @Override
  public void mergeWith(Store store) {
    if (store instanceof UnboundedSizeUDenseStore) {
      mergeWith((UnboundedSizeUDenseStore) store);
    } else {
      super.mergeWith(store);
    }
  }

  private void mergeWith(UnboundedSizeUDenseStore store) {

    if (store.isEmpty()) {
      return;
    }

    if (store.minIndex < minIndex || store.maxIndex > maxIndex) {
      extendRange(store.minIndex, store.maxIndex);
    }

    for (int index = store.minIndex; index <= store.maxIndex; index++) {
      counts[index - offset] += store.counts[index - store.offset];
    }
  }

  @Override
  public Store copy() {
    return new UnboundedSizeUDenseStore(this);
  }
}
