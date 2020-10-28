/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents context required for code generation. It maintains a stack of blocks
 * in the IR, that is used to correctly scope variable declarations and accesses.
 */
class TranslationContext {
    private static final String SUBQUERY_NAME_PREFIX = "subquery";
    private final Deque<OutputIR.Block> scopeStack;
    private final boolean isFunctionContext;
    private final AtomicInteger subqueryCounter = new AtomicInteger(0);

    private TranslationContext(final Deque<OutputIR.Block> declarations, final boolean isFunctionContext) {
        this.scopeStack = declarations;
        this.isFunctionContext = isFunctionContext;
    }

    TranslationContext(final boolean isFunctionContext) {
        this(new ArrayDeque<>(), isFunctionContext);
    }

    TranslationContext withEnterFunctionContext() {
        final Deque<OutputIR.Block> stackCopy = new ArrayDeque<>(scopeStack);
        return new TranslationContext(stackCopy, true);
    }

    boolean isFunctionContext() {
        return isFunctionContext;
    }

    void enterScope(final OutputIR.Block block) {
        scopeStack.addLast(block);
    }

    OutputIR.Block currentScope() {
        return Objects.requireNonNull(scopeStack.getLast());
    }

    OutputIR.Block leaveScope() {
        return scopeStack.removeLast();
    }

    OutputIR.Block getRootBlock() {
        return scopeStack.getFirst();
    }

    String declareVariable(final String expression) {
        for (final OutputIR.Block block: scopeStack) {
            if (block.hasDeclaration(expression)) {
                return block.getDeclaredName(expression);
            }
        }
        return scopeStack.getLast().declare(expression);
    }

    String declareVariable(final String expression, final OutputIR.Block block) {
        return block.declare(expression);
    }

    String getTupleVarName() {
        return currentScope().getTupleName();
    }

    String getNewSubqueryName() {
        return SUBQUERY_NAME_PREFIX + subqueryCounter.incrementAndGet();
    }
}
