
// WIP

#include "ha_mcs_opt_rewrites.h"

COND *
simplify_joins_(JOIN *join, List<TABLE_LIST> *join_list, COND *conds, bool top,
               bool in_sj)
{
  TABLE_LIST *table;
  NESTED_JOIN *nested_join;
  TABLE_LIST *prev_table= 0;
  List_iterator<TABLE_LIST> li(*join_list);
  bool straight_join= MY_TEST(join->select_options & SELECT_STRAIGHT_JOIN);
  DBUG_ENTER("simplify_joins");

  /* 
    Try to simplify join operations from join_list.
    The most outer join operation is checked for conversion first. 
  */
  while ((table= li++))
  {
    table_map used_tables;
    table_map not_null_tables= (table_map) 0;

    if ((nested_join= table->nested_join))
    {
      /* 
         If the element of join_list is a nested join apply
         the procedure to its nested join list first.
      */
      if (table->on_expr)
      {
        Item *expr= table->on_expr;
        /* 
           If an on expression E is attached to the table, 
           check all null rejected predicates in this expression.
           If such a predicate over an attribute belonging to
           an inner table of an embedded outer join is found,
           the outer join is converted to an inner join and
           the corresponding on expression is added to E. 
	*/ 
        expr= simplify_joins_(join, &nested_join->join_list,
                             expr, FALSE, in_sj || table->sj_on_expr);

        if (!table->prep_on_expr || expr != table->on_expr)
        {
          DBUG_ASSERT(expr);

          table->on_expr= expr;
          table->prep_on_expr= expr->copy_andor_structure(join->thd);
        }
      }
      nested_join->used_tables= (table_map) 0;
      nested_join->not_null_tables=(table_map) 0;
      conds= simplify_joins_(join, &nested_join->join_list, conds, top, 
                            in_sj || table->sj_on_expr);
      used_tables= nested_join->used_tables;
      not_null_tables= nested_join->not_null_tables;  
      /* The following two might become unequal after table elimination: */
      nested_join->n_tables= nested_join->join_list.elements;
    }
    else
    {
      if (!table->prep_on_expr)
        table->prep_on_expr= table->on_expr;
      used_tables= table->get_map();
      if (conds)
        not_null_tables= conds->not_null_tables();
    }
      
    if (table->embedding)
    {
      table->embedding->nested_join->used_tables|= used_tables;
      table->embedding->nested_join->not_null_tables|= not_null_tables;
    }

    if (!(table->outer_join & (JOIN_TYPE_LEFT | JOIN_TYPE_RIGHT)) ||
        (used_tables & not_null_tables))
    {
      /* 
        For some of the inner tables there are conjunctive predicates
        that reject nulls => the outer join can be replaced by an inner join.
      */
      if (table->outer_join && !table->embedding && table->table)
        table->table->maybe_null= FALSE;
      table->outer_join= 0;
      if (!(straight_join || table->straight))
      {
        table->dep_tables= 0;
        TABLE_LIST *embedding= table->embedding;
        while (embedding)
        {
          if (embedding->nested_join->join_list.head()->outer_join)
          {
            if (!embedding->sj_subq_pred)
              table->dep_tables= embedding->dep_tables;
            break;
          }
          embedding= embedding->embedding;
        }
      }
      if (table->on_expr)
      {
        /* Add ON expression to the WHERE or upper-level ON condition. */
        if (conds)
        {
          conds= and_conds(join->thd, conds, table->on_expr);
          conds->top_level_item();
          /* conds is always a new item as both cond and on_expr existed */
          DBUG_ASSERT(!conds->is_fixed());
          conds->fix_fields(join->thd, &conds);
        }
        else
          conds= table->on_expr; 
        table->prep_on_expr= table->on_expr= 0;
      }
    }

    /* 
      Only inner tables of non-convertible outer joins
      remain with on_expr.
    */ 
    if (table->on_expr)
    {
      table_map table_on_expr_used_tables= table->on_expr->used_tables();
      table->dep_tables|= table_on_expr_used_tables;
      if (table->embedding)
      {
        table->dep_tables&= ~table->embedding->nested_join->used_tables;   
        /*
           Embedding table depends on tables used
           in embedded on expressions. 
        */
        table->embedding->on_expr_dep_tables|= table_on_expr_used_tables;
      }
      else
        table->dep_tables&= ~table->get_map();
    }

    if (prev_table)
    {
      /* The order of tables is reverse: prev_table follows table */
      if (prev_table->straight || straight_join)
        prev_table->dep_tables|= used_tables;
      if (prev_table->on_expr)
      {
        prev_table->dep_tables|= table->on_expr_dep_tables;
        table_map prev_used_tables= prev_table->nested_join ?
	                            prev_table->nested_join->used_tables :
	                            prev_table->get_map();
        /* 
          If on expression contains only references to inner tables
          we still make the inner tables dependent on the outer tables.
          It would be enough to set dependency only on one outer table
          for them. Yet this is really a rare case.
          Note:
          RAND_TABLE_BIT mask should not be counted as it
          prevents update of inner table dependences.
          For example it might happen if RAND() function
          is used in JOIN ON clause.
	*/  
        if (!((prev_table->on_expr->used_tables() &
               ~(OUTER_REF_TABLE_BIT | RAND_TABLE_BIT)) &
              ~prev_used_tables))
          prev_table->dep_tables|= used_tables;
      }
    }
    prev_table= table;
  }
    
  /* 
    Flatten nested joins that can be flattened.
    no ON expression and not a semi-join => can be flattened.
  */
  li.rewind();
  while ((table= li++))
  {
    nested_join= table->nested_join;
    if (table->sj_on_expr && !in_sj)
    {
      /*
        If this is a semi-join that is not contained within another semi-join
        leave it intact (otherwise it is flattened)
      */
      /*
        Make sure that any semi-join appear in
        the join->select_lex->sj_nests list only once
      */
      List_iterator_fast<TABLE_LIST> sj_it(join->select_lex->sj_nests);
      TABLE_LIST *sj_nest;
      while ((sj_nest= sj_it++))
      {
        if (table == sj_nest)
          break;
      }
      if (sj_nest)
        continue;
      join->select_lex->sj_nests.push_back(table, join->thd->mem_root);

      /* 
        Also, walk through semi-join children and mark those that are now
        top-level
      */
      TABLE_LIST *tbl;
      List_iterator<TABLE_LIST> it(nested_join->join_list);
      while ((tbl= it++))
      {
        if (!tbl->on_expr && tbl->table)
          tbl->table->maybe_null= FALSE;
      }
    }
    else if (nested_join && !table->on_expr)
    {
      TABLE_LIST *tbl;
      List_iterator<TABLE_LIST> it(nested_join->join_list);
      List<TABLE_LIST> repl_list;  
      while ((tbl= it++))
      {
        tbl->embedding= table->embedding;
        if (!tbl->embedding && !tbl->on_expr && tbl->table)
          tbl->table->maybe_null= FALSE;
        tbl->join_list= table->join_list;
        repl_list.push_back(tbl, join->thd->mem_root);
        tbl->dep_tables|= table->dep_tables;
      }
      li.replace(repl_list);
    }
  }
  DBUG_RETURN(conds); 
}

//WIP
bool convert_join_subqueries_to_semijoins_(JOIN *join)
{
  Query_arena *arena, backup;
  Item_in_subselect *in_subq;
  THD *thd= join->thd;
  DBUG_ENTER("convert_join_subqueries_to_semijoins_");

  if (join->select_lex->sj_subselects.is_empty())
    DBUG_RETURN(FALSE);

  List_iterator_fast<Item_in_subselect> li(join->select_lex->sj_subselects);

  while ((in_subq= li++))
  {
    SELECT_LEX *subq_sel= in_subq->get_select_lex();
    if (subq_sel->handle_derived(thd->lex, DT_MERGE))
      DBUG_RETURN(TRUE);
    if (subq_sel->join->transform_in_predicates_into_in_subq(thd))
      DBUG_RETURN(TRUE);
    subq_sel->update_used_tables();
  }

  /* 
    Check all candidates to semi-join conversion that occur
    in ON expressions of outer join. Set the flag blocking
    this conversion for them.
  */
  TABLE_LIST *tbl;
  List_iterator<TABLE_LIST> ti(join->select_lex->leaf_tables);
  while ((tbl= ti++))
  {
    TABLE_LIST *embedded;
    TABLE_LIST *embedding= tbl;
    do
    {
      embedded= embedding;
      bool block_conversion_to_sj= false;
      if (embedded->on_expr)
      {
        /*
          Conversion of an IN subquery predicate into semi-join
          is blocked now if the predicate occurs:
          - in the ON expression of an outer join
          - in the ON expression of an inner join embedded directly
            or indirectly in the inner nest of an outer join
	*/
        for (TABLE_LIST *tl= embedded; tl; tl= tl->embedding)
	{
          if (tl->outer_join)
	  {
            block_conversion_to_sj= true;
            break;
          }
        }
      }
      if (block_conversion_to_sj)
      {
	Item *cond= embedded->on_expr;
        if (!cond)
          ;
        else if (cond->type() != Item::COND_ITEM)
          find_and_block_conversion_to_sj_(cond, li);
        else if (((Item_cond*) cond)->functype() ==
	         Item_func::COND_AND_FUNC)
	{
          Item *item;
          List_iterator<Item> it(*(((Item_cond*) cond)->argument_list()));
          while ((item= it++))
	  {
	    find_and_block_conversion_to_sj_(item, li);
          }
	}
      }
      embedding= embedded->embedding;
    }
    while (embedding &&
           embedding->nested_join->join_list.head() == embedded);
  }

  /* 
    Block conversion to semi-joins for those candidates that
    are encountered in the WHERE condition of the multi-table view
    with CHECK OPTION if this view is used in UPDATE/DELETE.
    (This limitation can be, probably, easily lifted.) 
  */  
  li.rewind();
  while ((in_subq= li++))
  {
    if (in_subq->emb_on_expr_nest != NO_JOIN_NEST &&
        in_subq->emb_on_expr_nest->effective_with_check)
    {
      in_subq->block_conversion_to_sj();
    }
  }

  if (join->select_options & SELECT_STRAIGHT_JOIN)
  {
    /* Block conversion to semijoins for all candidates */ 
    li.rewind();
    while ((in_subq= li++))
    {
      in_subq->block_conversion_to_sj();
    }
  }
      
  li.rewind();
  /* First, convert child join's subqueries. We proceed bottom-up here */
  while ((in_subq= li++)) 
  {
    st_select_lex *child_select= in_subq->get_select_lex();
    JOIN *child_join= child_select->join;
    child_join->outer_tables = child_join->table_count;

    /*
      child_select->where contains only the WHERE predicate of the
      subquery itself here. We may be selecting from a VIEW, which has its
      own predicate. The combined predicates are available in child_join->conds,
      which was built by setup_conds() doing prepare_where() for all views.
    */
    child_select->where= child_join->conds;

    if (convert_join_subqueries_to_semijoins_(child_join))
      DBUG_RETURN(TRUE);


    in_subq->sj_convert_priority= 
      MY_TEST(in_subq->do_not_convert_to_sj) * MAX_TABLES * 2 +
      in_subq->is_correlated * MAX_TABLES + child_join->outer_tables;
  }
  
  // Temporary measure: disable semi-joins when they are together with outer
  // joins.
#if 0  
  if (check_for_outer_joins(join->join_list))
  {
    in_subq= join->select_lex->sj_subselects.head();
    arena= thd->activate_stmt_arena_if_needed(&backup);
    goto skip_conversion;
  }
#endif
  //dump_TABLE_LIST_struct(select_lex, select_lex->leaf_tables);
  /* 
    2. Pick which subqueries to convert:
      sort the subquery array
      - prefer correlated subqueries over uncorrelated;
      - prefer subqueries that have greater number of outer tables;
  */
  bubble_sort<Item_in_subselect>(&join->select_lex->sj_subselects,
				 subq_sj_candidate_cmp_, NULL);
  // #tables-in-parent-query + #tables-in-subquery < MAX_TABLES
  /* Replace all subqueries to be flattened with Item_int(1) */
  arena= thd->activate_stmt_arena_if_needed(&backup);
 
  li.rewind();
  while ((in_subq= li++))
  {
    bool remove_item= TRUE;

    /* Stop processing if we've reached a subquery that's attached to the ON clause */
    if (in_subq->do_not_convert_to_sj)
    {
      break;
    }

    if (in_subq->is_flattenable_semijoin) 
    {
      if (join->table_count + 
          in_subq->unit->first_select()->join->table_count >= MAX_TABLES)
      {
        break;
      }
      if (convert_subq_to_sj_(join, in_subq))
        goto restore_arena_and_fail;
    }
    else
    {
      if (join->table_count + 1 >= MAX_TABLES)
        break;
      // WIP
      //if (convert_subq_to_jtbm(join, in_subq, &remove_item))
      //  goto restore_arena_and_fail;
    }
    if (remove_item)
    {
      Item **tree= (in_subq->emb_on_expr_nest == NO_JOIN_NEST)?
                     &join->conds : &(in_subq->emb_on_expr_nest->on_expr);
      Item *replace_me= in_subq->original_item();
      if (replace_where_subcondition_(join, tree, replace_me,
                                     new (thd->mem_root) Item_int(thd, 1),
                                     FALSE))
        goto restore_arena_and_fail;
    }
  }
//skip_conversion:
  /* 
    3. Finalize (perform IN->EXISTS rewrite) the subqueries that we didn't
    convert:
  */
  while (in_subq)
  {
    JOIN *child_join= in_subq->unit->first_select()->join;
    in_subq->changed= 0;
    in_subq->fixed= 0;

    SELECT_LEX *save_select_lex= thd->lex->current_select;
    thd->lex->current_select= in_subq->unit->first_select();

    bool res= in_subq->select_transformer(child_join);

    thd->lex->current_select= save_select_lex;

    if (res)
      DBUG_RETURN(TRUE);

    in_subq->changed= 1;
    in_subq->fixed= 1;

    Item *substitute= in_subq->substitution;
    bool do_fix_fields= !in_subq->substitution->is_fixed();
    Item **tree= (in_subq->emb_on_expr_nest == NO_JOIN_NEST)?
                   &join->conds : &(in_subq->emb_on_expr_nest->on_expr);
    Item *replace_me= in_subq->original_item();
    if (replace_where_subcondition_(join, tree, replace_me, substitute, 
                                   do_fix_fields))
      DBUG_RETURN(TRUE);
    in_subq->substitution= NULL;
    /*
      If this is a prepared statement, repeat the above operation for
      prep_where (or prep_on_expr). Subquery-to-semijoin conversion is 
      done once for prepared statement.
    */
    if (!thd->stmt_arena->is_conventional())
    {
      tree= (in_subq->emb_on_expr_nest == NO_JOIN_NEST)?
             &join->select_lex->prep_where : 
             &(in_subq->emb_on_expr_nest->prep_on_expr);
      /* 
        prep_on_expr/ prep_where may be NULL in some cases. 
        If that is the case, do nothing - simplify_joins() will copy 
        ON/WHERE expression into prep_on_expr/prep_where.
      */
      if (*tree && replace_where_subcondition_(join, tree, replace_me, substitute, 
                                     FALSE))
        DBUG_RETURN(TRUE);
    }
    /*
      Revert to the IN->EXISTS strategy in the rare case when the subquery could
      not be flattened.
    */
    in_subq->reset_strategy(SUBS_IN_TO_EXISTS);

    in_subq= li++;
  }

  if (arena)
    thd->restore_active_arena(arena, &backup);
  join->select_lex->sj_subselects.empty();
  DBUG_RETURN(FALSE);

restore_arena_and_fail:
  if (arena)
    thd->restore_active_arena(arena, &backup);
  DBUG_RETURN(TRUE);
}

bool replace_where_subcondition_(JOIN *join, Item **expr, 
                                       Item *old_cond, Item *new_cond,
                                       bool do_fix_fields)
{
  if (*expr == old_cond)
  {
    *expr= new_cond;
    if (do_fix_fields)
      new_cond->fix_fields(join->thd, expr);
    return FALSE;
  }
  
  if ((*expr)->type() == Item::COND_ITEM) 
  {
    List_iterator<Item> li(*((Item_cond*)(*expr))->argument_list());
    Item *item;
    while ((item= li++))
    {
      if (item == old_cond)
      {
        li.replace(new_cond);
        if (do_fix_fields)
          new_cond->fix_fields(join->thd, li.ref());
        return FALSE;
      }
      else if (item->type() == Item::COND_ITEM)
      {
        replace_where_subcondition_(join, li.ref(),
                                   old_cond, new_cond,
                                   do_fix_fields);
      }
    }
  }
  /* 
    We can come to here when 
     - we're doing replace operations on both on_expr and prep_on_expr
     - on_expr is the same as prep_on_expr, or they share a sub-tree 
       (so, when we do replace in on_expr, we replace in prep_on_expr, too,
        and when we try doing a replace in prep_on_expr, the item we wanted 
        to replace there has already been replaced)
  */
  return FALSE;
}

void find_and_block_conversion_to_sj_(Item *to_find,
				     List_iterator_fast<Item_in_subselect> &li)
{
  if (to_find->type() == Item::FUNC_ITEM &&
     ((Item_func*)to_find)->functype() == Item_func::IN_OPTIMIZER_FUNC)
    to_find= ((Item_in_optimizer*)to_find)->get_wrapped_in_subselect_item();

  if (to_find->type() != Item::SUBSELECT_ITEM ||
      ((Item_subselect *) to_find)->substype() != Item_subselect::IN_SUBS)
    return;
  Item_in_subselect *in_subq;
  li.rewind();
  while ((in_subq= li++))
  {
    if (in_subq == to_find)
    {
      in_subq->block_conversion_to_sj();
      return;
    }
  }
}

#if 0
bool convert_subq_to_jtbm_(JOIN *parent_join, 
                                 Item_in_subselect *subq_pred, 
                                 bool *remove_item)
{
  SELECT_LEX *parent_lex= parent_join->select_lex;
  List<TABLE_LIST> *emb_join_list= &parent_lex->top_join_list;
  TABLE_LIST *emb_tbl_nest= NULL; // will change when we learn to handle outer joins
  TABLE_LIST *tl;
  bool optimization_delayed= TRUE;
  TABLE_LIST *jtbm;
  LEX_STRING tbl_alias;
  THD *thd= parent_join->thd;
  DBUG_ENTER("convert_subq_to_jtbm");

  subq_pred->set_strategy(SUBS_MATERIALIZATION);
  subq_pred->is_jtbm_merged= TRUE;

  *remove_item= TRUE;

  if (!(tbl_alias.str= (char*)thd->calloc(SUBQERY_TEMPTABLE_NAME_MAX_LEN)) ||
      !(jtbm= alloc_join_nest(thd))) //todo: this is not a join nest!
  {
    DBUG_RETURN(TRUE);
  }

  jtbm->join_list= emb_join_list;
  jtbm->embedding= emb_tbl_nest;
  jtbm->jtbm_subselect= subq_pred;
  jtbm->nested_join= NULL;

  /* Nests do not participate in those 'chains', so: */
  /* jtbm->next_leaf= jtbm->next_local= jtbm->next_global == NULL*/
  emb_join_list->push_back(jtbm, thd->mem_root);
  
  /* 
    Inject the jtbm table into TABLE_LIST::next_leaf list, so that 
    make_join_statistics() and co. can find it.
  */
  parent_lex->leaf_tables.push_back(jtbm, thd->mem_root);

  if (subq_pred->unit->first_select()->options & OPTION_SCHEMA_TABLE)
    parent_lex->options |= OPTION_SCHEMA_TABLE;

  /*
    Same as above for TABLE_LIST::next_local chain
    (a theory: a next_local chain always starts with ::leaf_tables
     because view's tables are inserted after the view)
  */
  for (tl= (TABLE_LIST*)(parent_lex->table_list.first); tl->next_local; tl= tl->next_local)
  {}
  tl->next_local= jtbm;

  /* A theory: no need to re-connect the next_global chain */
  if (optimization_delayed)
  {
    DBUG_ASSERT(parent_join->table_count < MAX_TABLES);

    jtbm->jtbm_table_no= parent_join->table_count;

    create_subquery_temptable_name(&tbl_alias,
                                   subq_pred->unit->first_select()->select_number);
    jtbm->alias.str=    tbl_alias.str;
    jtbm->alias.length= tbl_alias.length;
    parent_join->table_count++;
    DBUG_RETURN(thd->is_fatal_error);
  }
  subselect_hash_sj_engine *hash_sj_engine=
    ((subselect_hash_sj_engine*)subq_pred->engine);
  jtbm->table= hash_sj_engine->tmp_table;

  jtbm->table->tablenr= parent_join->table_count;
  jtbm->table->map= table_map(1) << (parent_join->table_count);
  jtbm->jtbm_table_no= jtbm->table->tablenr;

  parent_join->table_count++;
  DBUG_ASSERT(parent_join->table_count < MAX_TABLES);

  Item *conds= hash_sj_engine->semi_join_conds;
  conds->fix_after_pullout(parent_lex, &conds, TRUE);

  DBUG_EXECUTE("where", print_where(conds,"SJ-EXPR", QT_ORDINARY););
  
  create_subquery_temptable_name(&tbl_alias, hash_sj_engine->materialize_join->
                                              select_lex->select_number);
  jtbm->alias.str=    tbl_alias.str;
  jtbm->alias.length= tbl_alias.length;

  parent_lex->have_merged_subqueries= TRUE;

  /* Don't unlink the child subselect, as the subquery will be used. */

  DBUG_RETURN(thd->is_fatal_error);
}
#endif

 int subq_sj_candidate_cmp_(Item_in_subselect* el1, Item_in_subselect* el2,
                                 void *arg)
{
  return (el1->sj_convert_priority > el2->sj_convert_priority) ? -1 : 
         ( (el1->sj_convert_priority == el2->sj_convert_priority)? 0 : 1);
}

bool convert_subq_to_sj_(JOIN *parent_join, Item_in_subselect *subq_pred)
{
  SELECT_LEX *parent_lex= parent_join->select_lex;
  TABLE_LIST *emb_tbl_nest= NULL;
  List<TABLE_LIST> *emb_join_list= &parent_lex->top_join_list;
  THD *thd= parent_join->thd;
  DBUG_ENTER("convert_subq_to_sj");

  /*
    1. Find out where to put the predicate into.
     Note: for "t1 LEFT JOIN t2" this will be t2, a leaf.
  */
  if ((void*)subq_pred->emb_on_expr_nest != (void*)NO_JOIN_NEST)
  {
    if (subq_pred->emb_on_expr_nest->nested_join)
    {
      /*
        We're dealing with

          ... [LEFT] JOIN  ( ... ) ON (subquery AND whatever) ...

        The sj-nest will be inserted into the brackets nest.
      */
      emb_tbl_nest=  subq_pred->emb_on_expr_nest;
      emb_join_list= &emb_tbl_nest->nested_join->join_list;
    }
    else if (!subq_pred->emb_on_expr_nest->outer_join)
    {
      /*
        We're dealing with

          ... INNER JOIN tblX ON (subquery AND whatever) ...

        The sj-nest will be tblX's "sibling", i.e. another child of its
        parent. This is ok because tblX is joined as an inner join.
      */
      emb_tbl_nest= subq_pred->emb_on_expr_nest->embedding;
      if (emb_tbl_nest)
        emb_join_list= &emb_tbl_nest->nested_join->join_list;
    }
    else if (!subq_pred->emb_on_expr_nest->nested_join)
    {
      TABLE_LIST *outer_tbl= subq_pred->emb_on_expr_nest;
      TABLE_LIST *wrap_nest;
      LEX_CSTRING sj_wrap_name= { STRING_WITH_LEN("(sj-wrap)") };
      /*
        We're dealing with

          ... LEFT JOIN tbl ON (on_expr AND subq_pred) ...

        we'll need to convert it into:

          ... LEFT JOIN ( tbl SJ (subq_tables) ) ON (on_expr AND subq_pred) ...
                        |                      |
                        |<----- wrap_nest ---->|
        
        Q:  other subqueries may be pointing to this element. What to do?
        A1: simple solution: copy *subq_pred->expr_join_nest= *parent_nest.
            But we'll need to fix other pointers.
        A2: Another way: have TABLE_LIST::next_ptr so the following
            subqueries know the table has been nested.
        A3: changes in the TABLE_LIST::outer_join will make everything work
            automatically.
      */
      if (!(wrap_nest= alloc_join_nest_(thd)))
      {
        DBUG_RETURN(TRUE);
      }
      wrap_nest->embedding= outer_tbl->embedding;
      wrap_nest->join_list= outer_tbl->join_list;
      wrap_nest->alias= sj_wrap_name;

      wrap_nest->nested_join->join_list.empty();
      wrap_nest->nested_join->join_list.push_back(outer_tbl, thd->mem_root);

      outer_tbl->embedding= wrap_nest;
      outer_tbl->join_list= &wrap_nest->nested_join->join_list;

      /*
        wrap_nest will take place of outer_tbl, so move the outer join flag
        and on_expr
      */
      wrap_nest->outer_join= outer_tbl->outer_join;
      outer_tbl->outer_join= 0;

      wrap_nest->on_expr= outer_tbl->on_expr;
      outer_tbl->on_expr= NULL;

      List_iterator<TABLE_LIST> li(*wrap_nest->join_list);
      TABLE_LIST *tbl;
      while ((tbl= li++))
      {
        if (tbl == outer_tbl)
        {
          li.replace(wrap_nest);
          break;
        }
      }
      /*
        Ok now wrap_nest 'contains' outer_tbl and we're ready to add the 
        semi-join nest into it
      */
      emb_join_list= &wrap_nest->nested_join->join_list;
      emb_tbl_nest=  wrap_nest;
    }
  }

  TABLE_LIST *sj_nest;
  NESTED_JOIN *nested_join;
  LEX_CSTRING sj_nest_name= { STRING_WITH_LEN("(sj-nest)") };
  if (!(sj_nest= alloc_join_nest_(thd)))
  {
    DBUG_RETURN(TRUE);
  }
  nested_join= sj_nest->nested_join;

  sj_nest->join_list= emb_join_list;
  sj_nest->embedding= emb_tbl_nest;
  sj_nest->alias= sj_nest_name;
  sj_nest->sj_subq_pred= subq_pred;
  sj_nest->original_subq_pred_used_tables= subq_pred->used_tables() |
                                           subq_pred->left_expr->used_tables();
  /* Nests do not participate in those 'chains', so: */
  /* sj_nest->next_leaf= sj_nest->next_local= sj_nest->next_global == NULL*/
  emb_join_list->push_back(sj_nest, thd->mem_root);

  /* 
    nested_join->used_tables and nested_join->not_null_tables are
    initialized in simplify_joins().
  */
  
  /* 
    2. Walk through subquery's top list and set 'embedding' to point to the
       sj-nest.
  */
  st_select_lex *subq_lex= subq_pred->unit->first_select();
  DBUG_ASSERT(subq_lex->next_select() == NULL);
  nested_join->join_list.empty();
  List_iterator_fast<TABLE_LIST> li(subq_lex->top_join_list);
  TABLE_LIST *tl;
  while ((tl= li++))
  {
    tl->embedding= sj_nest;
    tl->join_list= &nested_join->join_list;
    nested_join->join_list.push_back(tl, thd->mem_root);
  }
  
  /*
    Reconnect the next_leaf chain.
    TODO: Do we have to put subquery's tables at the end of the chain?
          Inserting them at the beginning would be a bit faster.
    NOTE: We actually insert them at the front! That's because the order is
          reversed in this list.
  */
  parent_lex->leaf_tables.append(&subq_lex->leaf_tables);

  if (subq_lex->options & OPTION_SCHEMA_TABLE)
    parent_lex->options |= OPTION_SCHEMA_TABLE;

  /*
    Same as above for next_local chain
    (a theory: a next_local chain always starts with ::leaf_tables
     because view's tables are inserted after the view)
  */
  
  for (tl= (TABLE_LIST*)(parent_lex->table_list.first); tl->next_local; tl= tl->next_local)
  {}

  tl->next_local= subq_lex->join->tables_list;

  /* A theory: no need to re-connect the next_global chain */

  /* 3. Remove the original subquery predicate from the WHERE/ON */

  // The subqueries were replaced for Item_int(1) earlier
  subq_pred->reset_strategy(SUBS_SEMI_JOIN);       // for subsequent executions
  /*TODO: also reset the 'm_with_subquery' there. */

  /* n. Adjust the parent_join->table_count counter */
  uint table_no= parent_join->table_count;
  /* n. Walk through child's tables and adjust table->map */
  List_iterator_fast<TABLE_LIST> si(subq_lex->leaf_tables);
  while ((tl= si++))
  {
    tl->set_tablenr(table_no);
    if (tl->is_jtbm())
    {
      tl->jtbm_table_no= table_no;
      Item *dummy= tl->jtbm_subselect;
      tl->jtbm_subselect->fix_after_pullout(parent_lex, &dummy, true);
      DBUG_ASSERT(dummy == tl->jtbm_subselect);
    }
    SELECT_LEX *old_sl= tl->select_lex;
    tl->select_lex= parent_join->select_lex; 
    for (TABLE_LIST *emb= tl->embedding;
         emb && emb->select_lex == old_sl;
         emb= emb->embedding)
      emb->select_lex= parent_join->select_lex;
    table_no++;
  }
  parent_join->table_count += subq_lex->join->table_count;
  //parent_join->table_count += subq_lex->leaf_tables.elements;

  /* 
    Put the subquery's WHERE into semi-join's sj_on_expr
    Add the subquery-induced equalities too.
  */
  SELECT_LEX *save_lex= thd->lex->current_select;
  thd->lex->current_select=subq_lex;
  if (subq_pred->left_expr->fix_fields_if_needed(thd, &subq_pred->left_expr))
    DBUG_RETURN(TRUE);
  thd->lex->current_select=save_lex;

  table_map subq_pred_used_tables= subq_pred->used_tables();
  sj_nest->nested_join->sj_corr_tables= subq_pred_used_tables;
  sj_nest->nested_join->sj_depends_on=  subq_pred_used_tables |
                                        subq_pred->left_expr->used_tables();
  sj_nest->sj_on_expr= subq_lex->join->conds;

  /*
    Create the IN-equalities and inject them into semi-join's ON expression.
    Additionally, for LooseScan strategy
     - Record the number of IN-equalities.
     - Create list of pointers to (oe1, ..., ieN). We'll need the list to
       see which of the expressions are bound and which are not (for those
       we'll produce a distinct stream of (ie_i1,...ie_ik).

       (TODO: can we just create a list of pointers and hope the expressions
       will not substitute themselves on fix_fields()? or we need to wrap
       them into Item_direct_view_refs and store pointers to those. The
       pointers to Item_direct_view_refs are guaranteed to be stable as 
       Item_direct_view_refs doesn't substitute itself with anything in 
       Item_direct_view_ref::fix_fields.
  */
  sj_nest->sj_in_exprs= subq_pred->left_expr->cols();
  sj_nest->nested_join->sj_outer_expr_list.empty();
  reset_equality_number_for_subq_conds_(sj_nest->sj_on_expr);

  if (subq_pred->left_expr->cols() == 1)
  {
    /* add left = select_list_element */
    nested_join->sj_outer_expr_list.push_back(&subq_pred->left_expr,
                                              thd->mem_root);
    /*
      Create Item_func_eq. Note that
      1. this is done on the statement, not execution, arena
      2. if it's a PS then this happens only once - on the first execution.
         On following re-executions, the item will be fix_field-ed normally.
      3. Thus it should be created as if it was fix_field'ed, in particular
         all pointers to items in the execution arena should be protected
         with thd->change_item_tree
    */
    Item_func_eq *item_eq=
      new (thd->mem_root) Item_func_eq(thd, subq_pred->left_expr_orig,
                                       subq_lex->ref_pointer_array[0]);
    if (!item_eq)
      DBUG_RETURN(TRUE);
    if (subq_pred->left_expr_orig != subq_pred->left_expr)
      thd->change_item_tree(item_eq->arguments(), subq_pred->left_expr);
    item_eq->in_equality_no= 0;
    sj_nest->sj_on_expr= and_items(thd, sj_nest->sj_on_expr, item_eq);
  }
  else if (subq_pred->left_expr->type() == Item::ROW_ITEM)
  {
    /*
      disassemple left expression and add
      left1 = select_list_element1 and left2 = select_list_element2 ...
    */
    for (uint i= 0; i < subq_pred->left_expr->cols(); i++)
    {
      nested_join->sj_outer_expr_list.push_back(subq_pred->left_expr->addr(i),
                                                thd->mem_root);
      Item_func_eq *item_eq=
        new (thd->mem_root)
        Item_func_eq(thd, subq_pred->left_expr_orig->element_index(i),
                     subq_lex->ref_pointer_array[i]);
      if (!item_eq)
        DBUG_RETURN(TRUE);
      DBUG_ASSERT(subq_pred->left_expr->element_index(i)->is_fixed());
      if (subq_pred->left_expr_orig->element_index(i) !=
          subq_pred->left_expr->element_index(i))
        thd->change_item_tree(item_eq->arguments(),
                              subq_pred->left_expr->element_index(i));
      item_eq->in_equality_no= i;
      sj_nest->sj_on_expr= and_items(thd, sj_nest->sj_on_expr, item_eq);
    }
  }
  else
  {
    /*
      add row operation
      left = (select_list_element1, select_list_element2, ...)
    */
    Item_row *row= new (thd->mem_root) Item_row(thd, subq_lex->pre_fix);
    /* fix fields on subquery was call so they should be the same */
    if (!row)
      DBUG_RETURN(TRUE);
    DBUG_ASSERT(subq_pred->left_expr->cols() == row->cols());
    nested_join->sj_outer_expr_list.push_back(&subq_pred->left_expr);
    Item_func_eq *item_eq=
      new (thd->mem_root) Item_func_eq(thd, subq_pred->left_expr_orig, row);
    if (!item_eq)
      DBUG_RETURN(TRUE);
    for (uint i= 0; i < row->cols(); i++)
    {
      if (row->element_index(i) != subq_lex->ref_pointer_array[i])
        thd->change_item_tree(row->addr(i), subq_lex->ref_pointer_array[i]);
    }
    item_eq->in_equality_no= 0;
    sj_nest->sj_on_expr= and_items(thd, sj_nest->sj_on_expr, item_eq);
  }
  /*
    Fix the created equality and AND

    Note that fix_fields() can actually fail in a meaningful way here. One
    example is when the IN-equality is not valid, because it compares columns
    with incompatible collations. (One can argue it would be more appropriate
    to check for this at name resolution stage, but as a legacy of IN->EXISTS
    we have in here).
  */
  if (sj_nest->sj_on_expr->fix_fields_if_needed(thd, &sj_nest->sj_on_expr))
  {
    DBUG_RETURN(TRUE);
  }

  /*
    Walk through sj nest's WHERE and ON expressions and call
    item->fix_table_changes() for all items.
  */
  sj_nest->sj_on_expr->fix_after_pullout(parent_lex, &sj_nest->sj_on_expr,
                                         TRUE);
  fix_list_after_tbl_changes(parent_lex, &sj_nest->nested_join->join_list);


  /* Unlink the child select_lex so it doesn't show up in EXPLAIN: */
  subq_lex->master_unit()->exclude_level();

  /* Inject sj_on_expr into the parent's WHERE or ON */
  if (emb_tbl_nest)
  {
    emb_tbl_nest->on_expr= and_items(thd, emb_tbl_nest->on_expr,
                                     sj_nest->sj_on_expr);
    emb_tbl_nest->on_expr->top_level_item();
    if (emb_tbl_nest->on_expr->fix_fields_if_needed(thd,
                                                    &emb_tbl_nest->on_expr))
    {
      DBUG_RETURN(TRUE);
    }
  }
  else
  {
    /* Inject into the WHERE */
    parent_join->conds= and_items(thd, parent_join->conds, sj_nest->sj_on_expr);
    parent_join->conds->top_level_item();
    /*
      fix_fields must update the properties (e.g. st_select_lex::cond_count of
      the correct select_lex.
    */
    save_lex= thd->lex->current_select;
    thd->lex->current_select=parent_join->select_lex;
    if (parent_join->conds->fix_fields_if_needed(thd, &parent_join->conds))
    {
      DBUG_RETURN(1);
    }
    thd->lex->current_select=save_lex;
    parent_join->select_lex->where= parent_join->conds;
  }

  if (subq_lex->ftfunc_list->elements)
  {
    Item_func_match *ifm;
    List_iterator_fast<Item_func_match> li(*(subq_lex->ftfunc_list));
    while ((ifm= li++))
      parent_lex->ftfunc_list->push_front(ifm, thd->mem_root);
  }

  parent_lex->have_merged_subqueries= TRUE;
  /* Fatal error may have been set to by fix_after_pullout() */
  DBUG_RETURN(thd->is_fatal_error);
}

TABLE_LIST *alloc_join_nest_(THD *thd)
{
  TABLE_LIST *tbl;
  if (!(tbl= (TABLE_LIST*) thd->calloc(ALIGN_SIZE(sizeof(TABLE_LIST))+
                                       sizeof(NESTED_JOIN))))
    return NULL;
  tbl->nested_join= (NESTED_JOIN*) ((uchar*)tbl + 
                                    ALIGN_SIZE(sizeof(TABLE_LIST)));
  return tbl;
}

void reset_equality_number_for_subq_conds_(Item * cond)
{
  if (!cond)
    return;
  if (cond->type() == Item::COND_ITEM)
  {
    List_iterator<Item> li(*((Item_cond*) cond)->argument_list());
    Item *item;
    while ((item=li++))
    {
      if (item->type() == Item::FUNC_ITEM &&
      ((Item_func*)item)->functype()== Item_func::EQ_FUNC)
        ((Item_func_eq*)item)->in_equality_no= UINT_MAX;
    }
  }
  else
  {
    if (cond->type() == Item::FUNC_ITEM &&
      ((Item_func*)cond)->functype()== Item_func::EQ_FUNC)
        ((Item_func_eq*)cond)->in_equality_no= UINT_MAX;
  }
  return;
}
