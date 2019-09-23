/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class MonoidComprehension extends Expr {
    @Nullable private Head head;
    private List<Qualifier> qualifiers = new ArrayList<>();

    MonoidComprehension() {
    }

    public MonoidComprehension(final Head head) {
        this.head = head;
    }

    public MonoidComprehension(final List<Qualifier> qualifiers) {
        this.qualifiers = qualifiers;
    }

    public MonoidComprehension(final Head head, final List<Qualifier> qualifiers) {
        this.head = head;
        this.qualifiers = qualifiers;
    }

    public MonoidComprehension withQualifier(final Qualifier qualifier) {
        final List<Qualifier> newQualifiers = new ArrayList<>(qualifiers);
        newQualifiers.add(qualifier);
        return new MonoidComprehension(head, newQualifiers);
    }

    @Nullable
    public Head getHead() {
        return head;
    }

    public List<Qualifier> getQualifiers() {
        return qualifiers;
    }

    @Override
    public String toString() {
        return  String.format("[%s | %s]", head, qualifiers);
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context) {
        return visitor.visitMonoidComprehension(this, context);
    }
}