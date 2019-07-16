package com.vrg.backend;

import com.vrg.compiler.UsesControllableFields;
import com.vrg.compiler.monoid.BinaryOperatorPredicate;
import com.vrg.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import com.vrg.compiler.monoid.Expr;
import com.vrg.compiler.monoid.GroupByComprehension;
import com.vrg.compiler.monoid.Head;
import com.vrg.compiler.monoid.MonoidComprehension;
import com.vrg.compiler.monoid.MonoidFunction;
import com.vrg.compiler.monoid.MonoidVisitor;
import com.vrg.compiler.monoid.Qualifier;
import com.vrg.compiler.monoid.TableRowGenerator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class RewriteArity {

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final GetVarQualifiers getVarQualifiers = new GetVarQualifiers();
        getVarQualifiers.visit(comprehension);
        return getVarQualifiers.result == null ? comprehension : getVarQualifiers.result;
    }

    private static class GetVarQualifiers extends MonoidVisitor<Void, Void> {
        private final List<Qualifier> varQualifiers = new ArrayList<>();
        private final List<Qualifier> nonVarQualifiers = new ArrayList<>();
        @Nullable private MonoidComprehension result;

        @Override
        protected Void visitGroupByComprehension(final GroupByComprehension node, final Void context) {
            final MonoidComprehension newInner = rewriteComprehension(node.getComprehension());
            result = new GroupByComprehension(newInner, node.getGroupByQualifier());
            return null;
        }

        @Override
        protected Void visitMonoidComprehension(final MonoidComprehension node, final Void context) {
            result = rewriteComprehension(node);
            return null;
        }

        @Override
        protected Void visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, final Void context) {
            switch (node.getOperator()) {
                case "==":
                case "<":
                case ">":
                case "<=":
                case ">=":
                case "!=": {
                    if (isControllableField(node.getLeft()) || isControllableField(node.getRight())) {
                        varQualifiers.add(node);
                    } else {
                        nonVarQualifiers.add(node);
                    }
                    super.visitBinaryOperatorPredicate(node, context);
                    break;
                }
                case "/\\": {
                    if (!isControllableField(node.getLeft()) && !isControllableField(node.getRight())) {
                        nonVarQualifiers.add(node);
                    }
                    super.visitBinaryOperatorPredicate(node, context);
                    break;
                }
                case "\\/": {
                    if (isControllableField(node.getLeft()) || isControllableField(node.getRight())) {
                        varQualifiers.add(node);
                    }
                    super.visitBinaryOperatorPredicate(node, context);
                    break;
                }
                default:
                    throw new RuntimeException("Missing case");
            }
            return null;
        }

        @Override
        protected Void visitTableRowGenerator(final TableRowGenerator node, final Void context) {
            nonVarQualifiers.add(node);
            return null;
        }

        /**
         * If input is not a sum() comprehension, ignore.
         */
        private MonoidComprehension rewriteComprehension(final MonoidComprehension input) {
            if (input.getHead() != null
                    && input.getHead().getSelectExprs().size() == 1
                    && input.getHead().getSelectExprs().get(0) instanceof MonoidFunction) {
                final MonoidFunction oldFunction = (MonoidFunction) input.getHead().getSelectExprs().get(0);
                if (oldFunction.getFunctionName().equalsIgnoreCase("sum")
                     || oldFunction.getFunctionName().equalsIgnoreCase("count")) {
                    // Extract var and non-var qualifiers
                    final GetVarQualifiers visitor = new GetVarQualifiers();
                    input.getQualifiers().forEach(visitor::visit);
                    final List<Qualifier> varQualifiers = visitor.varQualifiers;
                    // This is not a hard requirement for this optimization, but a temporary simplification
                    assert varQualifiers.size() == 1;

                    // Replace sum([col | var-q, non-var-q]) with sum([col * (var-q) | non-var-q])
                    final Expr oldSumArg = oldFunction.getArgument();
                    final BinaryOperatorPredicateWithAggregate newHeadItem
                            = new BinaryOperatorPredicateWithAggregate("*", oldSumArg, varQualifiers.get(0));
                    final MonoidFunction newFunction = new MonoidFunction(oldFunction.getFunctionName(), newHeadItem);
                    oldFunction.getAlias().ifPresent(newFunction::setAlias);
                    final Head newHead = new Head(Collections.singletonList(newFunction));
                    final MonoidComprehension newInner = new MonoidComprehension(newHead);
                    newInner.addQualifiers(visitor.nonVarQualifiers);
                    return newInner;
                }
            }
            System.err.println("Did not rewrite comprehension: " + input);
            return input;
        }

        private boolean isControllableField(final Expr expr) {
            final UsesControllableFields usesControllableFields = new UsesControllableFields();
            usesControllableFields.visit(expr);
            return usesControllableFields.usesControllableFields();
        }
    }
}