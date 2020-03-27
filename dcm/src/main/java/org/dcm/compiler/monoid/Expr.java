/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.compiler.monoid;

import javax.annotation.Nullable;
import java.util.Optional;

public abstract class Expr {
    private Optional<String> alias = Optional.empty();

    @Nullable
    abstract <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, @Nullable final C context);

    public void setAlias(final String alias) {
        this.alias = Optional.of(alias);
    }

    public Optional<String> getAlias() {
        return alias;
    }
}
