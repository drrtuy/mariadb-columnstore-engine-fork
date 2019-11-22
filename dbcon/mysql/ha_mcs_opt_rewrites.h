// WIP
#ifndef HA_MCS_REWRITES
#define HA_MCS_REWRITES

#include "idb_mysql.h"

COND *simplify_joins_(JOIN *join, List<TABLE_LIST> *join_list, COND *conds, bool top, bool in_sj);
bool convert_join_subqueries_to_semijoins_(JOIN *join);
bool replace_where_subcondition_(JOIN *join, Item **expr, 
                                       Item *old_cond, Item *new_cond,
                                       bool do_fix_fields);
void find_and_block_conversion_to_sj_(Item *to_find,
				     List_iterator_fast<Item_in_subselect> &li);

#if 0
bool convert_subq_to_jtbm_(JOIN *parent_join, 
                                 Item_in_subselect *subq_pred, 
                                 bool *remove_item);
#endif
int subq_sj_candidate_cmp_(Item_in_subselect* el1, Item_in_subselect* el2,
                                 void *arg);

bool convert_subq_to_sj_(JOIN *parent_join, Item_in_subselect *subq_pred);
TABLE_LIST *alloc_join_nest_(THD *thd);
void reset_equality_number_for_subq_conds_(Item * cond);
#endif

