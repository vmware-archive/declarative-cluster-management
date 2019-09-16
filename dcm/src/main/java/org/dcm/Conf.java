/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class Conf {

    private final Map<String, String> params = new HashMap<>();

    @Nullable
    public String getProperty(final String key) {
        return params.get(key);
    }

    public void setProperty(final String key, final String value) {
        params.put(key, value);
    }
}
