/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.minizinc;

import com.vmware.dcm.compiler.ir.GroupByComprehension;
import com.vmware.dcm.compiler.ir.Head;
import com.vmware.dcm.compiler.ir.ListComprehension;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Splits a comprehension with multiple select items into multiple comprehensions, each with a single select item.
 */
class SplitIntoSingleHeadComprehensions {

    static List<ListComprehension> apply(final ListComprehension input) {
        if (input instanceof GroupByComprehension) {
            final GroupByComprehension groupByComprehension = (GroupByComprehension) input;
            final ListComprehension innerComprehension = groupByComprehension.getComprehension();
            return innerComprehension.getHead()
                    .getSelectExprs()
                    .stream()
                    .map(e -> {
                        final ListComprehension mc =
                                new ListComprehension(new Head(Collections.singletonList(e)),
                                                        innerComprehension.getQualifiers());
                        return new GroupByComprehension(mc, groupByComprehension.getGroupByQualifier());
                    })
                    .collect(Collectors.toList());
        }
        else {
            return input.getHead()
                    .getSelectExprs()
                    .stream()
                    .map(e -> new ListComprehension(new Head(Collections.singletonList(e)),
                            input.getQualifiers()))
                    .collect(Collectors.toList());
        }
    }
}
