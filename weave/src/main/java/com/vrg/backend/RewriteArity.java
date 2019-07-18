package com.vrg.backend;

import com.vrg.compiler.UsesControllableFields;
import com.vrg.compiler.monoid.BinaryOperatorPredicate;
import com.vrg.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.ComprehensionRewriter;
import com.vrg.compiler.monoid.Expr;
import com.vrg.compiler.monoid.GroupByComprehension;
import com.vrg.compiler.monoid.MonoidComprehension;
import com.vrg.compiler.monoid.MonoidFunction;
import com.vrg.compiler.monoid.MonoidLiteral;
import com.vrg.compiler.monoid.MonoidVisitor;
import com.vrg.compiler.monoid.Qualifier;
import com.vrg.compiler.monoid.TableRowGenerator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class RewriteArity {

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final ArityRewriter rewriter = new ArityRewriter();
        final Expr result = Objects.requireNonNull(rewriter.visit(comprehension));
        return comprehension instanceof GroupByComprehension ?
                (GroupByComprehension) result : (MonoidComprehension) result;
    }

    private static class ArityRewriter extends ComprehensionRewriter<Void> {
        @Override
        protected Expr visitMonoidComprehension(final MonoidComprehension node, final Void context) {
            return rewriteComprehension(node);
        }

        /**
         * If input is not a sum() comprehension, ignore.
         */
        private MonoidComprehension rewriteComprehension(final MonoidComprehension input) {
            // Extract var and non-var qualifiers
            final GetVarQualifiers visitor = new GetVarQualifiers();
            input.getQualifiers().forEach(visitor::visit);
            final List<Qualifier> varQualifiers = visitor.varQualifiers;
            final List<Qualifier> nonVarQualifiers = visitor.nonVarQualifiers;
            if (varQualifiers.isEmpty()) {
                return input;
            }
            if (varQualifiers.size() != 1) {
                System.err.println("Found multiple var qualifiers. Skipping arity rewrite.");
                for (final Qualifier qualifier: varQualifiers) {
                    System.err.println("--- " + qualifier);
                }
                return input;
            }
            final MonoidComprehension comprehensionWithoutVarQualifiers =
                    new MonoidComprehension(input.getHead(), nonVarQualifiers);
            final FunctionRewriter functionRewriter = new FunctionRewriter();
            final MonoidComprehension result =
                    (MonoidComprehension) functionRewriter.visit(comprehensionWithoutVarQualifiers,
                                                                 varQualifiers.get(0));
            return functionRewriter.didRewrite ? Objects.requireNonNull(result) : input;
        }
    }

    private static class GetVarQualifiers extends MonoidVisitor<Void, Void> {
        private final List<Qualifier> varQualifiers = new ArrayList<>();
        private final List<Qualifier> nonVarQualifiers = new ArrayList<>();

        @Nullable
        @Override
        protected Void visitGroupByComprehension(final GroupByComprehension node, @Nullable Void context) {
            return null;
        }

        @Nullable
        @Override
        protected Void visitMonoidComprehension(final MonoidComprehension node, @Nullable Void context) {
            return null;
        }

        @Override
        protected Void visitTableRowGenerator(final TableRowGenerator node, final Void context) {
            nonVarQualifiers.add(node);
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
                case "in":
                case "\\/": {
                    if (isControllableField(node.getLeft()) || isControllableField(node.getRight())) {
                        varQualifiers.add(node);
                    }
                    super.visitBinaryOperatorPredicate(node, context);
                    break;
                }
                default:
                    throw new RuntimeException("Missing case " + node.getOperator());
            }
            return null;
        }

        private boolean isControllableField(final Expr expr) {
            final UsesControllableFields usesControllableFields = new UsesControllableFields();
            usesControllableFields.visit(expr);
            return usesControllableFields.usesControllableFields();
        }
    }

    private static class FunctionRewriter extends ComprehensionRewriter<Qualifier> {
        private boolean didRewrite = false;

        @Override
        protected Expr visitGroupByComprehension(final GroupByComprehension node, @Nullable final Qualifier context) {
            return node;
        }

        @Override
        protected Expr visitMonoidComprehension(final MonoidComprehension node, @Nullable final Qualifier context) {
            return node;
        }

        @Override
        protected Expr visitMonoidFunction(final MonoidFunction node, @Nullable final Qualifier qualifier) {
            assert qualifier != null;
            assert node.getArgument() instanceof ColumnIdentifier || node.getArgument() instanceof MonoidLiteral;
            // Replace sum([col | var-q, non-var-q]) with sum([col * (var-q) | non-var-q])
            if (node.getFunctionName().equalsIgnoreCase("sum") ||
                node.getFunctionName().equalsIgnoreCase("count")) {
                final Expr oldSumArg = node.getArgument();
                final BinaryOperatorPredicateWithAggregate newArgument
                        = new BinaryOperatorPredicateWithAggregate("*", oldSumArg, qualifier);
                didRewrite = true;
                return new MonoidFunction(node.getFunctionName(), newArgument);
            }
            return node;
        }
    }
}