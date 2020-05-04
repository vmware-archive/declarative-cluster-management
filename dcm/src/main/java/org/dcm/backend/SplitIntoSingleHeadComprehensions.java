/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.GroupByComprehension;
import org.dcm.compiler.monoid.Head;
import org.dcm.compiler.monoid.MonoidComprehension;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Splits a comprehension with multiple select items into multiple comprehensions, each with a single select item.
 */
class SplitIntoSingleHeadComprehensions {

    static List<MonoidComprehension> apply(final MonoidComprehension input) {
        if (input.getHead() != null) {
            return input.getHead()
                    .getSelectExprs()
                    .stream()
                    .map(e -> new MonoidComprehension(new Head(Collections.singletonList(e)),
                                                      input.getQualifiers()))
                    .collect(Collectors.toList());
        }
        else if (input instanceof GroupByComprehension) {
            final GroupByComprehension groupByComprehension = (GroupByComprehension) input;
            final MonoidComprehension innerComprehension = groupByComprehension.getComprehension();
            return innerComprehension.getHead()
                    .getSelectExprs()
                    .stream()
                    .map(e -> {
                        final MonoidComprehension mc =
                                new MonoidComprehension(new Head(Collections.singletonList(e)),
                                                        innerComprehension.getQualifiers());
                        return new GroupByComprehension(mc, groupByComprehension.getGroupByQualifier());
                    })
                    .collect(Collectors.toList());
        }
        else {
            return Collections.singletonList(input);
        }
    }
}
