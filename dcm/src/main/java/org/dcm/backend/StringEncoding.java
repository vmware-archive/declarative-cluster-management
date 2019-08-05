/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;

public class StringEncoding {
    private static final int DEFAULT_CAPACITY = 1000;
    private int counter = 0;
    private final Object2LongArrayMap<String> strToLongMap = new Object2LongArrayMap<>(DEFAULT_CAPACITY);
    private final Long2ObjectArrayMap<String> longToStrMap = new Long2ObjectArrayMap<>(DEFAULT_CAPACITY);

    public long toLong(final String str) {
        return strToLongMap.computeIfAbsent(str, this::update);
    }

    // Pass through
    public long toLong(final long i) {
        return i;
    }

    public String toStr(final long i) {
        return longToStrMap.get(i);
    }

    final long update(final String str) {
        counter += 1;
        longToStrMap.put(counter, str);
        return counter;
    }
}