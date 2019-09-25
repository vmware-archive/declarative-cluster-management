/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1ResourceRequirements;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Utils {

    // By default, we always assign a request size of 1 milli CPU or 1KB.
    static double resourceRequirementSum(final List<V1ResourceRequirements> resourceRequirements,
                                          final String resourceName) {
        return resourceRequirements.stream().mapToDouble(e -> {
            if (e.getRequests() == null) {
                return 1L;
            }
            final Quantity resourceQuantity = e.getRequests().get(resourceName);
            if (resourceQuantity == null) {
                return 1L;
            }
            return convertUnit(resourceQuantity, resourceName);
        }).sum();
    }

    static double convertUnit(final Quantity quantity, final String resourceName) {
        final String res = quantity.toSuffixedString().replaceAll("[a-zA-Z]", "");
        final double baseAmount = Double.parseDouble(res);
        final Pattern p = Pattern.compile("[a-zA-Z]+");
        final Matcher m = p.matcher(quantity.toSuffixedString());
        if (m.find()) {
            final String unit = m.group();
            switch (unit) {
                case "m":
                    return baseAmount;
                case "Ki":
                    return baseAmount;
                case "Mi":
                    return baseAmount * Math.pow(2, 10);
                case "Gi":
                    return baseAmount * Math.pow(2, 20);
                case "Ti":
                    return baseAmount * Math.pow(2, 30);
                case "Pi":
                    // XXX: Likely to overflow
                    return baseAmount * Math.pow(2, 40);
                case "Ei":
                    // XXX: Likely to overflow
                    return baseAmount * Math.pow(2, 50);
                default:
                    return baseAmount / Math.pow(2, 10);
            }
        } else {
            if (resourceName.equals("cpu")) {
                return baseAmount * 1000; // we represent CPU in milli-CPU units.
            }
            return baseAmount;
        }
    }
}
