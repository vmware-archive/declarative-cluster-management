/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.backend.ortools;

import com.vmware.dcm.compiler.ir.GroupByQualifier;
import edu.umd.cs.findbugs.annotations.Nullable;

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
    @Nullable private final GroupContext groupContext;
    @Nullable private final SubQueryContext subQueryContext;
    private final AtomicInteger subqueryCounter;

    private TranslationContext(final Deque<OutputIR.Block> declarations, final boolean isFunctionContext,
                               final GroupContext groupContext, final SubQueryContext subQueryContext,
                               final AtomicInteger subqueryCounter) {
        this.scopeStack = declarations;
        this.isFunctionContext = isFunctionContext;
        this.groupContext = groupContext;
        this.subQueryContext = subQueryContext;
        this.subqueryCounter = subqueryCounter;
    }

    TranslationContext(final boolean isFunctionContext) {
        this(new ArrayDeque<>(), isFunctionContext, null, null, new AtomicInteger(0));
    }

    /**
     * Takes an expression, and declares it as an intermediate variable in the current context. It then
     * returns the declared variable name.
     *
     * @param expression expression to assign to a variable
     * @param type type of the expression
     * @return An intermediate variable name
     */
    JavaExpression declare(final String expression, final JavaType type) {
        return new JavaExpression(declareVariable(expression), type);
    }

    /**
     * Takes an expression, and declares it as an intermediate variable in the current context. It then
     * returns the declared variable name.
     *
     * @param identifier a typed expression to assign to a variable
     * @return An intermediate variable name
     */
    JavaExpression declare(final JavaExpression identifier) {
        return new JavaExpression(declareVariable(identifier.asString()), identifier.type());
    }

    TranslationContext withEnterFunctionContext() {
        final Deque<OutputIR.Block> stackCopy = new ArrayDeque<>(scopeStack);
        return new TranslationContext(stackCopy, true, groupContext, subQueryContext, subqueryCounter);
    }

    TranslationContext withEnterSubQueryContext(final String newSubqueryName) {
        final SubQueryContext subQueryContext = new SubQueryContext(newSubqueryName);
        final Deque<OutputIR.Block> stackCopy = new ArrayDeque<>(scopeStack);
        return new TranslationContext(stackCopy, isFunctionContext, groupContext, subQueryContext, subqueryCounter);
    }

    TranslationContext withEnterGroupContext(final GroupByQualifier qualifier, final String tempTableName,
                                             final String groupViewName) {
        final GroupContext groupContext = new GroupContext(qualifier, tempTableName, groupViewName);
        final Deque<OutputIR.Block> stackCopy = new ArrayDeque<>(scopeStack);
        return new TranslationContext(stackCopy, isFunctionContext, groupContext, subQueryContext, subqueryCounter);
    }

    boolean isFunctionContext() {
        return isFunctionContext;
    }

    public boolean isGroupContext() {
        return groupContext != null;
    }

    public GroupContext getGroupContext() {
        return Objects.requireNonNull(groupContext);
    }

    public boolean isSubQueryContext() {
        return subQueryContext != null;
    }

    public SubQueryContext getSubQueryContext() {
        return Objects.requireNonNull(subQueryContext);
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
