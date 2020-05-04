/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import java.util.HashMap;

public class StringEncoding {
    private static final int DEFAULT_CAPACITY = 10000;
    private long counter = 0;
    private final HashMap<String, Long> strToLongMap = new HashMap<>(DEFAULT_CAPACITY);
    private final HashMap<Long, String> longToStrMap = new HashMap<>(DEFAULT_CAPACITY);

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