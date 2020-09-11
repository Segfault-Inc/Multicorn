


#ifndef PG_QUERY_H
#define PG_QUERY_H

List * extractColumns(List *reltargetlist, List *restrictinfolist);
void initConversioninfo(ConversionInfo ** cinfos, AttInMetadata *attinmeta);
char * getOperatorString(Oid opoid);
Node * unnestClause(Node *node);
void swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid, Relids base_relids);
OpExpr * canonicalOpExpr(OpExpr *opExpr, Relids base_relids);
ScalarArrayOpExpr * canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr, Relids base_relids);
void extractRestrictions(Relids base_relids, Expr *node, List **quals);
void extractClauseFromOpExpr(Relids base_relids, OpExpr *op, List **quals);
void extractClauseFromScalarArrayOpExpr(Relids base_relids, ScalarArrayOpExpr *op, List **quals);
void extractClauseFromNullTest(Relids base_relids, NullTest *node, List **quals);
Value * colnameFromVar(Var *var, PlannerInfo *root, MulticornPlanState * planstate);
MulticornBaseQual * makeQual(AttrNumber varattno, char *opname, Expr *value, bool isarray, bool useOr);
bool isAttrInRestrictInfo(Index relid, AttrNumber attno, RestrictInfo *restrictinfo);
List * clausesInvolvingAttr(Index relid, AttrNumber attnum, EquivalenceClass *ec);
void computeDeparsedSortGroup(List *deparsed, MulticornPlanState *planstate, List **apply_pathkeys, List **deparsed_pathkeys);
List * findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths, int startupCost, MulticornPlanState *state, List *apply_pathkeys, List *deparsed_pathkeys);
List * deparse_sortgroup(PlannerInfo *root, Oid foreigntableid, RelOptInfo *rel);
Expr * multicorn_get_em_expr(EquivalenceClass *ec, RelOptInfo *rel);
List * serializeDeparsedSortGroup(List *pathkeys);
List * deserializeDeparsedSortGroup(List *items);

#endif
