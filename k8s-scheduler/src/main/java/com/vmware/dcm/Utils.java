/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.List;

class Utils {

    static long resourceRequirementSum(final List<ResourceRequirements> resourceRequirements,
                                             final String resourceName) {
        return resourceRequirements.stream().mapToLong(e -> {
            if (e == null || e.getRequests() == null) {
                return 0L;
            }
            final Quantity resourceQuantity = e.getRequests().get(resourceName);
            if (resourceQuantity == null) {
                return 0L;
            }
            return convertUnit(resourceQuantity, resourceName);
        }).sum();
    }

    static long convertUnit(final Quantity quantity, final String resourceName) {
        if (resourceName.equals("cpu")) {
            final String unit = quantity.getFormat();
            if (unit.equals("")) {
                // Convert to milli-cpu
                return (long) (Double.parseDouble(quantity.getAmount()) * 1000);
            } else if (unit.equals("m")) {
                return (long) (Double.parseDouble(quantity.getAmount()));
            }
            throw new IllegalArgumentException(quantity + " for resource type: " + resourceName);
        } else {
            // Values are guaranteed to be under 2^63 - 1, so this is safe
            return Quantity.getAmountInBytes(quantity).longValue();
        }
    }
}