#include "multicorn.h"
#include "optimizer/var.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "mb/pg_wchar.h"
#include "utils/lsyscache.h"

const char *getEncodingFromAttribute(Form_pg_attribute attribute);


void extractClauseFromOpExpr(PlannerInfo *root,
						RelOptInfo *baserel,
						OpExpr *node,
						List **quals,
						List **params);

void extractClauseFromNullTest(PlannerInfo *root,
						  RelOptInfo *baserel,
						  NullTest *node,
						  List **quals,
						  List **params);

void extractClauseFromScalarArrayOpExpr(PlannerInfo *root,
								   RelOptInfo *baserel,
								   ScalarArrayOpExpr *node,
								   List **quals,
								   List **params);

char	   *getOperatorString(Oid opoid);

List *makeQual(AttrNumber varattno, char *opname, Expr *value,
		 bool isarray,
		 bool useOr);


Node	   *unnestClause(Node *node);
void swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid,
					 RelOptInfo *baserel);
OpExpr	   *canonicalOpExpr(OpExpr *opExpr, RelOptInfo *baserel);
ScalarArrayOpExpr *canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr,
						   RelOptInfo *baserel);

bool isAttrInRestrictInfo(Index relid, AttrNumber attno,
					 RestrictInfo *restrictinfo);

List *clausesInvolvingAttr(Index relid, AttrNumber attnum,
					 EquivalenceClass *eq_class);

/*
 * The list of needed columns (represented by their respective vars)
 * is pulled from:
 *	- the targetcolumns
 *	- the restrictinfo
 */
List *
extractColumns(PlannerInfo *root, RelOptInfo *baserel)
{
	ListCell   *lc;
	List	   *columns = NULL;

	foreach(lc, baserel->reltargetlist)
	{
		List	   *targetcolumns;
		Node	   *node = (Node *) lfirst(lc);

		targetcolumns = pull_var_clause(node,
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
		columns = list_union(columns, targetcolumns);
	}
	foreach(lc, baserel->baserestrictinfo)
	{
		List	   *targetcolumns;
		RestrictInfo *node = (RestrictInfo *) lfirst(lc);

		targetcolumns = pull_var_clause((Node *) node->clause,
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
		columns = list_union(columns, targetcolumns);
	}
	return columns;
}

/*
 * Initialize the array of "ConversionInfo" elements, needed to convert python
 * objects back to suitable postgresql data structures.
 */
void
initConversioninfo(ConversionInfo ** cinfos, AttInMetadata *attinmeta)
{
	int			i;

	for (i = 0; i < attinmeta->tupdesc->natts; i++)
	{
		Form_pg_attribute attr = attinmeta->tupdesc->attrs[i];

		if (!attr->attisdropped)
		{
			ConversionInfo *cinfo = palloc0(sizeof(ConversionInfo));

			cinfo->atttypoid = attr->atttypid;
			cinfo->atttypmod = attinmeta->atttypmods[i];
			cinfo->attioparam = attinmeta->attioparams[i];
			cinfo->attinfunc = &attinmeta->attinfuncs[i];
			cinfo->encodingname = getEncodingFromAttribute(attr);
			cinfo->attrname = NameStr(attr->attname);
			cinfo->attnum = i + 1;
			cinfos[i] = cinfo;
		}
		else
		{
			cinfos[i] = NULL;
		}
	}
}



/*
 * Get a (python) encoding name for an attribute.
 */
const char *
getEncodingFromAttribute(Form_pg_attribute attribute)
{
	HeapTuple	tp;
	Form_pg_collation colltup;
	const char *encoding_name;

	tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(attribute->attcollation));
	if (!HeapTupleIsValid(tp))
		return "ascii";
	colltup = (Form_pg_collation) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	if (colltup->collencoding == -1)
	{
		/* No encoding information, do stupid things */
		encoding_name = GetDatabaseEncodingName();
	}
	else
	{
		encoding_name = (char *) pg_encoding_to_char(colltup->collencoding);
	}
	if (strcmp(encoding_name, "SQL_ASCII") == 0)
	{
		encoding_name = "ascii";
	}
	return encoding_name;
}

char *
getOperatorString(Oid opoid)
{
	HeapTuple	tp;
	Form_pg_operator operator;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for operator %u", opoid);
	operator = (Form_pg_operator) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	return NameStr(operator->oprname);
}


/*
 * Returns the node of interest from a node.
 */
Node *
unnestClause(Node *node)
{
	switch (node->type)
	{
		case T_RelabelType:
			return (Node *) ((RelabelType *) node)->arg;
		case T_ArrayCoerceExpr:
			return (Node *) ((ArrayCoerceExpr *) node)->arg;
		default:
			return node;
	}
}


void
swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid,
					 RelOptInfo *baserel)
{
	HeapTuple	tp;
	Form_pg_operator op;
	Relids		base_relids = baserel->relids;
	Node	   *l = *left,
			   *r = *right;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(*opoid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for operator %u", *opoid);
	op = (Form_pg_operator) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	/* Right is already a var. */
	/* If "left" is a Var from another rel, and right is a Var from the */
	/* target rel, swap them. */
	/* Same thing is left is not a var at all. */
	/* To swap them, we have to lookup the commutator operator. */
	if (IsA(r, Var))
	{
		Var		   *rvar = (Var *) r;

		if (!IsA(l, Var) ||
			(!bms_is_member(((Var *) l)->varno, base_relids) &&
			 bms_is_member(rvar->varno, base_relids)))
		{
			/* If the operator has no commutator operator, */
			/* bail out. */
			if (op->oprcom == 0)
			{
				return;
			}
			{
				*left = r;
				*right = l;
				*opoid = op->oprcom;
			}
		}
	}

}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
OpExpr *
canonicalOpExpr(OpExpr *opExpr, RelOptInfo *baserel)
{
	Oid			operatorid = opExpr->opno;
	Node	   *l,
			   *r;
	OpExpr	   *result = NULL;
	Relids		base_relids = baserel->relids;

	/* Only treat binary operators for now. */
	if (list_length(opExpr->args) == 2)
	{
		l = unnestClause(list_nth(opExpr->args, 0));
		r = unnestClause(list_nth(opExpr->args, 1));
		swapOperandsAsNeeded(&l, &r, &operatorid, baserel);
	}
	if (IsA(l, Var) &&bms_is_member(((Var *) l)->varno, base_relids))
	{
		result = (OpExpr *) make_opclause(operatorid,
										  opExpr->opresulttype,
										  opExpr->opretset,
										  (Expr *) l, (Expr *) r,
										  opExpr->opcollid,
										  opExpr->inputcollid);
	}
	return result;
}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
ScalarArrayOpExpr *
canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr,
						   RelOptInfo *baserel)
{
	Oid			operatorid = opExpr->opno;
	Node	   *l,
			   *r;
	ScalarArrayOpExpr *result = opExpr;
	Relids		base_relids = baserel->relids;
	HeapTuple	tp;
	Form_pg_operator op;

	/* Only treat binary operators for now. */
	if (list_length(opExpr->args) == 2)
	{
		l = unnestClause(list_nth(opExpr->args, 0));
		r = unnestClause(list_nth(opExpr->args, 1));
		swapOperandsAsNeeded(&l, &r, &operatorid, baserel);
		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for operator %u", operatorid);
		op = (Form_pg_operator) GETSTRUCT(tp);
		ReleaseSysCache(tp);
		if (IsA(l, Var) &&bms_is_member(((Var *) l)->varno, base_relids))
		{
			result = makeNode(ScalarArrayOpExpr);
			result->opno = operatorid;
			result->opfuncid = op->oprcode;
			result->useOr = opExpr->useOr;
			result->args = lappend(result->args, l);
			result->args = lappend(result->args, r);
			result->location = opExpr->location;

		}
	}
	return result;
}


/*
 * Extract conditions that can be pushed down, as well as the parameters.
 *
 */
void
extractRestrictions(PlannerInfo *root,
					RelOptInfo *baserel,
					Expr *node,
					List **quals,
					List **params)
{
	switch (nodeTag(node))
	{
		case T_OpExpr:
			extractClauseFromOpExpr(root, baserel,
									(OpExpr *) node, quals, params);
			break;
		case T_NullTest:
			extractClauseFromNullTest(root, baserel,
									  (NullTest *) node, quals, params);
			break;
		case T_ScalarArrayOpExpr:
			extractClauseFromScalarArrayOpExpr(root, baserel,
											   (ScalarArrayOpExpr *) node,
											   quals,
											   params);
			break;
		default:
			{
				ereport(WARNING,
						(errmsg("unsupported expression for "
								"extractClauseFrom"),
						 errdetail("%s", nodeToString(node))));
			}
			break;
	}
}

/*
 *	Build an intermediate value representation for an OpExpr,
 *	and append it to the corresponding list (quals, or params).
 *
 *	The quals list consist of list of the form:
 *
 *	- Const key: the column index in the cinfo array
 *	- Const operator: the operator representation
 *	- Var or Const value: the value.
 */
void
extractClauseFromOpExpr(PlannerInfo *root,
						RelOptInfo *baserel,
						OpExpr *op,
						List **quals,
						List **params)
{
	Var		   *left;
	Expr	   *right;

	/* Use a "canonical" version of the op expression, to ensure that the */
	/* left operand is a Var on our relation. */
	op = canonicalOpExpr(op, baserel);
	if (op)
	{
		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		switch (right->type)
		{
				/* The simplest case: somevar == a constant */
			case T_Const:
				*quals = lappend(*quals, makeQual(left->varattno,
												  getOperatorString(op->opno),
												  right, false, false));
				break;
				/* Param: somevar == an outer param. */
			case T_Param:
				*params = lappend(*params, makeQual(left->varattno,
												 getOperatorString(op->opno),
													right, false, false));
				break;
				/* Var: somevar == someothervar. */
			case T_Var:
				/* This job could/should? be done in the core by */
				/* replace_nestloop_vars. Infortunately, this is private. */
				if (bms_is_member(((Var *) right)->varno, root->curOuterRels))
				{
					Param	   *param = assign_nestloop_param_var(root,
															  (Var *) right);

					*params = lappend(*params, makeQual(left->varattno,
												 getOperatorString(op->opno),
														(Expr *) param,
														false,
														false));
				}
				break;
				/* Ignore other node types. */
			default:
				break;
		}
	}
}

void
extractClauseFromScalarArrayOpExpr(PlannerInfo *root,
								   RelOptInfo *baserel,
								   ScalarArrayOpExpr *op,
								   List **quals,
								   List **params)
{
	Var		   *left;
	Expr	   *right;

	op = canonicalScalarArrayOpExpr(op, baserel);
	if (op)
	{
		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		switch (right->type)
		{
				/* The simplest case: somevar == a constant */
			case T_Const:
				*quals = lappend(*quals, makeQual(left->varattno,
												  getOperatorString(op->opno),
												  right, true,
												  op->useOr));
				break;
				/* Param: somevar == an outer param. */
			case T_Param:
				*params = lappend(*params, makeQual(left->varattno,
												 getOperatorString(op->opno),
													right, false, false));
				break;
				/* Var: somevar == someothervar. */
			case T_Var:
				break;
				/* Ignore other node types. */
			default:
				break;
		}
	}
}


/*
 *	Convert a "NullTest" (IS NULL, or IS NOT NULL)
 *	to a suitable intermediate representation.
 */
void
extractClauseFromNullTest(PlannerInfo *root,
						  RelOptInfo *baserel,
						  NullTest *node,
						  List **quals,
						  List **params)
{
	if (IsA(node->arg, Var))
	{
		Var		   *var = (Var *) node->arg;
		List	   *result;
		char	   *opname = NULL;

		if (node->nulltesttype == IS_NULL)
		{
			opname = "=";
		}
		else
		{
			opname = "<>";
		}
		result = makeQual(var->varattno, opname,
						  (Expr *) makeNullConst(INT4OID, -1, InvalidOid),
						  false,
						  false);
		*quals = lappend(*quals, result);
	}
}



/*
 *	Returns a "Value" node containing the string name of the column from a var.
 */
Value *
colnameFromVar(Var *var, PlannerInfo *root)
{
	RangeTblEntry *rte = root->simple_rte_array[var->varno];
	char	   *attname = get_attname(rte->relid, var->varattno);

	if (attname == NULL)
	{
		return makeString("");
	}
	else
	{
		return makeString(attname);
	}
}

/*
 *	Build an opaque "qual" object.
 */
List *
makeQual(AttrNumber varattno, char *opname, Expr *value, bool isarray,
		 bool useOr)
{
	List	   *result = NULL;

	result = lappend_int(result, varattno - 1);
	result = lappend(result, makeString(opname));
	result = lappend(result, copyObject(value));
	result = lappend_int(result, isarray);
	result = lappend_int(result, useOr);
	return result;
}

/*
 *	Test wheter an attribute identified by its relid and attno
 *	is present in a list of restrictinfo
 */
bool
isAttrInRestrictInfo(Index relid, AttrNumber attno, RestrictInfo *restrictinfo)
{
	List	   *vars = pull_var_clause((Node *) restrictinfo->clause,
									   PVC_RECURSE_AGGREGATES,
									   PVC_RECURSE_PLACEHOLDERS);
	ListCell   *lc;

	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		if (var->varno == relid && var->varattno == attno)
		{
			return true;
		}

	}
	return false;
}

List *
clausesInvolvingAttr(Index relid, AttrNumber attnum,
					 EquivalenceClass *ec)
{
	List	   *clauses = NULL;

	/*
	 * If there is only one member, then the equivalence class is either for
	 * an outer join, or a desired sort order. So we better leave it
	 * untouched.
	 */
	if (ec->ec_members->length > 1)
	{
		ListCell   *ri_lc;

		foreach(ri_lc, ec->ec_sources)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(ri_lc);

			if (isAttrInRestrictInfo(relid, attnum, ri))
			{
				clauses = lappend(clauses, ri);
			}
		}
	}
	return clauses;
}

void
findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths)
{
	ListCell   *lc;

	foreach(lc, possiblePaths)
	{
		List	   *item = lfirst(lc);
		List	   *attrnos = linitial(item);
		ListCell   *attno_lc;
		int			nbrows = lsecond_int(item);
		List	   *allclauses = NULL;
		Bitmapset  *outer_relids = NULL;

		/* Armed with this knowledge, look for a join condition */
		/* matching the path list. */
		/* Every key must be present in either, a join clause or an */
		/* equivalence_class. */
		foreach(attno_lc, attrnos)
		{
			AttrNumber	attnum = lfirst_int(attno_lc);
			ListCell   *lc;
			List	   *clauses = NULL;

			/* Look in the equivalence classes. */
			foreach(lc, root->eq_classes)
			{
				EquivalenceClass *ec = (EquivalenceClass *) lfirst(lc);
				List	   *ec_clauses = clausesInvolvingAttr(baserel->relid,
															  attnum,
															  ec);

				clauses = list_concat(clauses, ec_clauses);
				if (ec_clauses != NIL)
				{
					outer_relids = bms_union(outer_relids, ec->ec_relids);
				}
			}
			/* Do the same thing for the outer joins */
			foreach(lc, list_union(root->left_join_clauses,
								   root->right_join_clauses))
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (isAttrInRestrictInfo(baserel->relid, attnum, ri))
				{
					clauses = lappend(clauses, ri);
					outer_relids = bms_union(outer_relids,
											 ri->outer_relids);

				}
			}
			/* We did NOT find anything for this key, bail out */
			if (clauses == NIL)
			{
				allclauses = NULL;
				break;
			}
			else
			{
				allclauses = list_concat(allclauses, clauses);
			}
		}
		/* Every key has a corresponding restriction, we can build */
		/* the parameterized path and add it to the plan. */
		if (allclauses != NIL)
		{
			Bitmapset  *req_outer = bms_difference(outer_relids,
										 bms_make_singleton(baserel->relid));
			ParamPathInfo *ppi;
			ForeignPath *foreignPath;

			if (!bms_is_empty(req_outer))
			{
				ppi = makeNode(ParamPathInfo);
				ppi->ppi_req_outer = req_outer;
				ppi->ppi_rows = nbrows;
				ppi->ppi_clauses = list_concat(ppi->ppi_clauses, allclauses);
				foreignPath = create_foreignscan_path(
													  root, baserel,
													  nbrows,
									  baserel->baserestrictcost.startup + 10,
													  nbrows * baserel->width,
													  NIL,
													  NULL,
													  NULL);

				foreignPath->path.param_info = ppi;
				add_path(baserel, (Path *) foreignPath);
			}
		}
	}
}
