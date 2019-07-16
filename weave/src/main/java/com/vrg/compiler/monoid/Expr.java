/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg.compiler.monoid;

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
