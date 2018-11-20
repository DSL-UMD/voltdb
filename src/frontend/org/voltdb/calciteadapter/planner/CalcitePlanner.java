/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.planner;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rules.VoltDBRules;
import org.voltdb.calciteadapter.util.VoltDBRelUtil;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

public class CalcitePlanner {

    // Set to keep rules that are added for a specific query and need to be removed
    // once the query is compiled
    // Thread safety???
    private static Set<RelOptRule> queryRuleSet = new HashSet<>();

    private static SchemaPlus schemaPlusFromDatabase(Database db) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        for (Table table : db.getTables()) {
            rootSchema.add(table.getTypeName(), new VoltDBTable(table));
        }

        return rootSchema;
    }

    private static Planner getVolcanoPlanner(SchemaPlus schema) {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(schema)
                .programs(VoltDBRules.getVolcanoPrograms())
                .build();
          return Frameworks.getPlanner(config);
    }

    private static HepPlanner getHepPlanner() {
        final HepProgramBuilder hepPgmBldr = new HepProgramBuilder();
        for (RelOptRule hepRule : VoltDBRules.INLINING_RULES) {
            hepPgmBldr.addRuleInstance(hepRule);
        }
        final HepPlanner planner = new HepPlanner(hepPgmBldr.build());
        planner.addRelTraitDef(VoltDBPRel.VOLTDB_PHYSICAL.getTraitDef());
        return planner;
    }


    private static CompiledPlan calciteToVoltDBPlan(VoltDBPRel rel, CompiledPlan compiledPlan) {

        RexConverter.resetParameterIndex();

        AbstractPlanNode root = rel.toPlanNode();
        assert(root instanceof SendPlanNode);

        compiledPlan.rootPlanGraph = root;

        PostBuildVisitor postPlannerVisitor = new PostBuildVisitor();
        root.acceptVisitor(postPlannerVisitor);

        compiledPlan.setReadOnly(true);
        compiledPlan.statementGuaranteesDeterminism(
                postPlannerVisitor.hasLimitOffset(), // no limit or offset
                postPlannerVisitor.isOrderDeterministic(),  // is order deterministic
                null); // no details on determinism

        compiledPlan.setStatementPartitioning(StatementPartitioning.forceSP());

        compiledPlan.setParameters(postPlannerVisitor.getParameterValueExpressions());

        return compiledPlan;
    }


    public static CompiledPlan plan(Database db, String sql, String dirName, boolean isLargeQuery) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        SchemaPlus schema = schemaPlusFromDatabase(db);
        Planner volcanoPlanner = getVolcanoPlanner(schema);
        HepPlanner hepPlanner = null;
        CompiledPlan compiledPlan = new CompiledPlan(isLargeQuery);

        compiledPlan.sql = sql;

        SqlNode parsedSql = null;
        SqlNode validatedSql = null;
        RelNode convertedRel = null;
        RelTraitSet traitSet = null;
        RelNode phaseOneRel = null;
        RelNode phaseTwoRel = null;
        RelNode phaseThreeRel = null;
        RelTrait collationTrait = null;
        String errMsg = null;

        // Set VoltDB Metadata Provider
        JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(
                VoltDBDefaultRelMetadataProvider.INSTANCE);
        RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);

        try {
            // Parse the input sql
            parsedSql = volcanoPlanner.parse(sql);

            // Validate the input sql
            validatedSql = volcanoPlanner.validate(parsedSql);

            // Convert the input sql to a relational expression
            convertedRel = volcanoPlanner.rel(validatedSql).project();
            // Get a possible collation trait if any
            collationTrait = convertedRel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

            // Transform the relational expression

            // Set the RelMetaProvider for every RelNode in the SQL operator Rel tree.
            convertedRel.accept(new VoltDBDefaultRelMetadataProvider.MetaDataProviderModifier(relMetadataProvider));
            // Prepare the set of RelTraits required of the root node at the termination of the planning phase
            traitSet = prepareOutputTraitSet(volcanoPlanner, VoltDBLRel.VOLTDB_LOGICAL, collationTrait);
            // Apply Rule set 0 - standard Calcite transformations and convert to the VOLTDB Logical convention
            phaseOneRel = volcanoPlanner.transform(0, traitSet, convertedRel);

            // Apply Rule Set 1 - VoltDB transformations

            // Set the RelMetaProvider for every RelNode in the Rel tree.
            phaseOneRel.accept(new VoltDBDefaultRelMetadataProvider.MetaDataProviderModifier(relMetadataProvider));

            // Starting from this phase (VoltDB Physical) we are going to require Calcite to produce a plan
            // that must have a root with a RelDistribution.SINGLELTON trait set.

            // Add RelDistribution trait definition to the planner to make Calcite aware of the new trait.
            phaseOneRel.getCluster().getPlanner().addRelTraitDef(RelDistributions.SINGLETON.getTraitDef());

            // Prepare the set of RelTraits required of the root node at the termination of the planning phase
            traitSet = prepareOutputTraitSet(volcanoPlanner, VoltDBPRel.VOLTDB_PHYSICAL,
                    collationTrait, RelDistributions.SINGLETON);

            // Add RelDistributions.ANY trait to the rel tree. It will be replaced with a real trait
            // during the transformation
            phaseOneRel = VoltDBRelUtil.addTraitRecurcively(phaseOneRel, RelDistributions.ANY);

            // Transform the rel
            phaseTwoRel = volcanoPlanner.transform(1, traitSet, phaseOneRel);

            // Apply Rule Set 2 - VoltDB inlining
            hepPlanner = getHepPlanner();

            // Set the RelMetaProvider for every RelNode in the Rel tree.
            phaseTwoRel.accept(new VoltDBDefaultRelMetadataProvider.MetaDataProviderModifier(relMetadataProvider));

            hepPlanner.setRoot(phaseTwoRel);
            phaseThreeRel = hepPlanner.findBestExp();

            // Convert To VoltDB plan
            calciteToVoltDBPlan((VoltDBPRel)phaseThreeRel, compiledPlan);

            String explainPlan = compiledPlan.rootPlanGraph.toExplainPlanString();

            compiledPlan.explainedPlan = explainPlan;
            // Renumber the plan node ids to start with 1
            compiledPlan.resetPlanNodeIds(1);

            PlanDebugOutput.outputPlanFullDebug(compiledPlan, compiledPlan.rootPlanGraph,
                    dirName, "JSON");
            PlanDebugOutput.outputExplainedPlan(compiledPlan, dirName, "CALCITE");
        }
        catch (Throwable e) {
            errMsg = e.getMessage();
            System.out.println("For some reason planning failed!..And here's the error:");
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.out.println("And here's how far we have gotten:\n");
            throw new PlanningErrorException(e.getMessage());
        } finally {
            volcanoPlanner.close();
            volcanoPlanner.reset();

            if (hepPlanner != null) {
                hepPlanner.clear();
            }

            PlanDebugOutput.outputCalcitePlanningDetails(
                    sql, dirName, "DEBUG", errMsg,
                    parsedSql, validatedSql, convertedRel,
                    phaseOneRel, phaseTwoRel, phaseThreeRel);
        }
        return compiledPlan;
    }

    // Prepare a trait set that would contains traits that we want to see in the root node
    // at the termination of the planning cycle.
    private static RelTraitSet prepareOutputTraitSet(
            Planner planner,
            Convention outConvention,
            RelTrait collationTrait,
            RelTrait...otherTraits) {
        RelTraitSet traitSet = planner.getEmptyTraitSet().replace(outConvention);
        // If a RelNode does not have a real RelCollation trait Calcite returns
        // an empty collation (RelCompositeTrait$EmptyCompositeTrait<T>)
        // which is not an instance of the RelCollation class (RelCollations.EMPTY) as
        // a T RelTraitSet.getTrait(RelTraitDef<T> traitDef) method declaration implies
        // resulting in a ClassCastExpretion
        if (collationTrait instanceof RelCollation) {
            traitSet = traitSet.plus(collationTrait);
        }

        // Add other traits
        if (otherTraits != null && otherTraits.length > 0) {
            traitSet = traitSet.plusAll(otherTraits);
        }

        return traitSet;
    }

    /**
     * Add a new rule to the current rule set
     * @param planner
     * @param newRule
     * @return
     */
    public static boolean addRule(RelOptPlanner planner, RelOptRule newRule) {
        boolean result = planner.addRule(newRule);
        if (result) {
            queryRuleSet.add(newRule);
        }
        return result;
    }

    /**
     * Remove all query specific rules that were added during planning.
     * Not sure though at what time to invoke it. It requires the access to the RelOptPlanner
     * which is not available from the org.apache.calcite.tools.Planner - private member.
     * Any rule has access to the RelOptPlanner
     *
     * @param planner
     */
    public static void clearQueryRules(RelOptPlanner planner) {
        for(RelOptRule rule : queryRuleSet) {
            planner.removeRule(rule);
        }
    }
}
