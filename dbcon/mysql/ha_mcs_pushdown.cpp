/*
   Copyright (c) 2019 MariaDB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <typeinfo>
#include <string>

#include "ha_mcs_pushdown.h"

// WIP 
COND *simplify_joins_(JOIN *join, List<TABLE_LIST> *join_list, COND *conds, bool top, bool in_sj);

void check_walk(const Item* item, void* arg);

void mutate_optimizer_flags(THD *thd_)
{
    // MCOL-2178 Disable all optimizer flags as it was in the fork.
    // CS restores it later in SH::scan_end() and in case of an error
    // in SH::scan_init()
    set_original_optimizer_flags(thd_->variables.optimizer_switch, thd_);
    thd_->variables.optimizer_switch = OPTIMIZER_SWITCH_IN_TO_EXISTS |
        OPTIMIZER_SWITCH_COND_PUSHDOWN_FOR_DERIVED |
        OPTIMIZER_SWITCH_COND_PUSHDOWN_FROM_HAVING;
}

void restore_optimizer_flags(THD *thd_)
{
    // MCOL-2178 restore original optimizer flags after SH, DH
    ulonglong orig_flags = get_original_optimizer_flags(thd_);
    if (orig_flags)
    {
        thd_->variables.optimizer_switch = orig_flags;
        set_original_optimizer_flags(0, thd_);
    }
}

int save_group_list(THD* thd, SELECT_LEX* select_lex, Group_list_ptrs **group_list_ptrs)
{
    if (select_lex->group_list.first)
    {
        void *mem = thd->stmt_arena->alloc(sizeof(Group_list_ptrs));

        if (!mem || !((*group_list_ptrs) = new (mem) Group_list_ptrs(thd->stmt_arena->mem_root)) ||
            (*group_list_ptrs)->reserve(select_lex->group_list.elements))
        {
            return 1;
        }

        for (ORDER *order = select_lex->group_list.first; order; order = order->next)
        {
            (*group_list_ptrs)->push_back(order);
        }
    }
    return 0;
}

void restore_group_list(SELECT_LEX* select_lex, Group_list_ptrs *group_list_ptrs)
{
    select_lex->group_list.empty();
    for (size_t i = 0; i < group_list_ptrs->size(); i++)
    {
        ORDER *order = (*group_list_ptrs)[i];
        select_lex->group_list.link_in_list(order, &order->next);
    }
}

/*@brief  find_tables - This traverses Item              */
/**********************************************************
* DESCRIPTION:
* This f() pushes TABLE of an Item_field to a list
* provided. The list is used for JOIN predicate check in
* is_joinkeys_predicate().
* PARAMETERS:
*    Item * Item to check
* RETURN:
***********************************************************/
void find_tables(const Item* item, void* arg)
{
    if (typeid(*item) == typeid(Item_field))
    {
        Item_field *ifp= (Item_field*)item;
        List<TABLE> *tables_list= (List<TABLE>*)arg;
        tables_list->push_back(ifp->field->table);
    }
}

/*@brief  is_joinkeys_predicate - This traverses Item_func*/
/***********************************************************
* DESCRIPTION:
* This f() walks Item_func and checks whether it contains
* JOIN predicate 
* PARAMETERS:
*    Item_func * Item to walk
* RETURN:
*    BOOL false if Item_func isn't a JOIN predicate
*    BOOL true otherwise
***********************************************************/
bool is_joinkeys_predicate(const Item_func *ifp)
{
    bool result = false;
    if(ifp->argument_count() == 2)
    {
        if (ifp->arguments()[0]->type() == Item::FIELD_ITEM &&
          ifp->arguments()[1]->type() == Item::FIELD_ITEM)
        {
            Item_field* left= reinterpret_cast<Item_field*>(ifp->arguments()[0]);
            Item_field* right= reinterpret_cast<Item_field*>(ifp->arguments()[1]);

            // If MDB crashes here with non-fixed Item_field and field == NULL
            // there must be a check over on_expr for a different SELECT_LEX.
            // e.g. checking subquery with ON from upper query. 
            if (left->field->table != right->field->table)
            {
                result= true;
            }
        }
        else
        {
            List<TABLE>llt; List<TABLE>rlt;
            Item *left= ifp->arguments()[0];
            Item *right= ifp->arguments()[1];
            // Search for tables inside left and right expressions
            // and compare them
            left->traverse_cond(find_tables, (void*)&llt, Item::POSTFIX);
            right->traverse_cond(find_tables, (void*)&rlt, Item::POSTFIX);
            // TODO Find the way to have more then one element or prove
            // the idea is useless.
            if (llt.elements && rlt.elements && (llt.elem(0) != rlt.elem(0)))
            {
                result= true;
            }
        }
    }
    return result;
}

/*@brief  find_nonequi_join - This traverses Item          */
/************************************************************
* DESCRIPTION:
* This f() walks Item and looks for a non-equi join
* predicates
* PARAMETERS:
*    Item * Item to walk
* RETURN:
***********************************************************/
void find_nonequi_join(const Item* item, void *arg)
{
    bool *unsupported_feature  = reinterpret_cast<bool*>(arg);
    if ( *unsupported_feature )
        return;

    if (item->type() == Item::FUNC_ITEM)
    {
        const Item_func* ifp = reinterpret_cast<const Item_func*>(item);
        //TODO Check for IN
        //NOT IN + correlated subquery
        if (ifp->functype() != Item_func::EQ_FUNC)
        {
            if (is_joinkeys_predicate(ifp))
                *unsupported_feature = true;
            else if (ifp->functype() == Item_func::NOT_FUNC
                       && ifp->arguments()[0]->type() == Item::EXPR_CACHE_ITEM)
            {
                check_walk(ifp->arguments()[0], arg);
            }
        }
    }
}

/*@brief  find_join - This traverses Item                  */
/************************************************************
* DESCRIPTION:
* This f() walks traverses Item looking for JOIN, SEMI-JOIN
* predicates. 
* PARAMETERS:
*    Item * Item to traverse
* RETURN:
***********************************************************/
void find_join(const Item* item, void* arg)
{
    bool *unsupported_feature  = reinterpret_cast<bool*>(arg);
    if ( *unsupported_feature )
        return;

    if (item->type() == Item::FUNC_ITEM)
    {
        const Item_func* ifp = reinterpret_cast<const Item_func*>(item);
        //TODO Check for IN
        //NOT IN + correlated subquery
        {
            if (is_joinkeys_predicate(ifp))
                *unsupported_feature = true;
            else if (ifp->functype() == Item_func::NOT_FUNC
                       && ifp->arguments()[0]->type() == Item::EXPR_CACHE_ITEM)
            {
                check_walk(ifp->arguments()[0], arg);
            }
        }
    }
}

/*@brief  save_join_predicate - This traverses Item        */
/************************************************************
* DESCRIPTION:
* This f() walks Item and saves found JOIN predicates into
* a List. The list will be used for a simple CROSS JOIN
* check in create_DH.
* PARAMETERS:
*    Item * Item to walk
* RETURN:
***********************************************************/
void save_join_predicates(const Item* item, void* arg)
{
    if (item->type() == Item::FUNC_ITEM)
    {
        const Item_func* ifp= reinterpret_cast<const Item_func*>(item);
        if (is_joinkeys_predicate(ifp))
        {
            List<Item> *join_preds_list= (List<Item>*)arg;
            join_preds_list->push_back(const_cast<Item*>(item));
        }
    }
}


/*@brief  check_walk - It traverses filter conditions      */
/************************************************************
* DESCRIPTION:
* It traverses filter predicates looking for unsupported
* JOIN types: non-equi JOIN, e.g  t1.c1 > t2.c2;
* logical OR.
* PARAMETERS:
*    thd - THD pointer.
*    derived - TABLE_LIST* to work with.
* RETURN:
***********************************************************/
void check_walk(const Item* item, void* arg)
{
    bool *unsupported_feature  = reinterpret_cast<bool*>(arg);
    if ( *unsupported_feature )
        return;

    switch (item->type())
    {
        case Item::FUNC_ITEM:
        {
            find_nonequi_join(item, arg);
            break;
        }
        case Item::EXPR_CACHE_ITEM: // IN + correlated subquery
        {
            const Item_cache_wrapper* icw = reinterpret_cast<const Item_cache_wrapper*>(item);
            if ( icw->get_orig_item()->type() == Item::FUNC_ITEM )
            {
                const Item_func *ifp = reinterpret_cast<const Item_func*>(icw->get_orig_item());
                if ( ifp->argument_count() == 2 &&
                    ( ifp->arguments()[0]->type() == Item::Item::SUBSELECT_ITEM
                    || ifp->arguments()[1]->type() == Item::Item::SUBSELECT_ITEM ))
                {
                    *unsupported_feature = true;
                    return;
                }
            }
            break;
        }
        case Item::COND_ITEM: // OR contains JOIN conds thats is unsupported yet
        {
            Item_cond* icp = (Item_cond*)item;
            if ( is_cond_or(icp) )
            {
                bool left_flag= false, right_flag= false;
                if (icp->argument_list()->elements >= 2)
                {
                    Item *left; Item *right;
                    List_iterator<Item> li(*icp->argument_list());
                    left = li++; right = li++;
                    left->traverse_cond(find_join, (void*)&left_flag, Item::POSTFIX);
                    right->traverse_cond(find_join, (void*)&right_flag, Item::POSTFIX);
                    if (left_flag && right_flag)
                    {
                        *unsupported_feature = true;
                    }
                }
            }
            break;
        }
        default:
        {
            break;
        }
    }
}

/*@brief  check_user_var_func - This traverses Item        */
/************************************************************
* DESCRIPTION:
* This f() walks Item looking for the existence of
* "set_user_var" or "get_user_var" functions.
* PARAMETERS:
*    Item * Item to traverse
* RETURN:
***********************************************************/
void check_user_var_func(const Item* item, void* arg)
{
    bool* unsupported_feature  = reinterpret_cast<bool*>(arg);

    if (*unsupported_feature)
        return;

    if (item->type() == Item::FUNC_ITEM)
    {
        const Item_func* ifp = reinterpret_cast<const Item_func*>(item);
        std::string funcname = ifp->func_name();
        if (funcname == "set_user_var" || funcname == "get_user_var")
        {
            *unsupported_feature = true;
        }
    }
}

void item_check(Item* item, bool* unsupported_feature)
{
    switch (item->type())
    {
        case Item::COND_ITEM:
        {
            Item_cond *icp = reinterpret_cast<Item_cond*>(item);
            icp->traverse_cond(check_user_var_func, unsupported_feature, Item::POSTFIX);
            break;
        }
        case Item::FUNC_ITEM:
        {
            Item_func *ifp = reinterpret_cast<Item_func*>(item);
            ifp->traverse_cond(check_user_var_func, unsupported_feature, Item::POSTFIX);
            break;
        }
        default:
        {
            item->traverse_cond(check_user_var_func, unsupported_feature, Item::POSTFIX);
            break;
        }
    }
}

/*@brief  create_columnstore_group_by_handler- Creates handler*/
/***********************************************************
 * DESCRIPTION:
 * Creates a group_by pushdown handler if there is no:
 *  non-equi JOIN, e.g * t1.c1 > t2.c2
 *  logical OR in the filter predicates
 *  Impossible WHERE
 *  Impossible HAVING
 * and there is either GROUP BY or aggregation function
 * exists at the top level.
 * Valid queries with the last two crashes the server if
 * processed.
 * Details are in server/sql/group_by_handler.h
 * PARAMETERS:
 *    thd - THD pointer
 *    query - Query structure LFM in group_by_handler.h
 * RETURN:
 *    group_by_handler if success
 *    NULL in other case
 ***********************************************************/
group_by_handler*
create_columnstore_group_by_handler(THD* thd, Query* query)
{
    ha_mcs_group_by_handler* handler = NULL;

    // same as thd->lex->current_select
    SELECT_LEX *select_lex = query->from->select_lex;

    // MCOL-2178 Disable SP support in the group_by_handler for now
    // Check the session variable value to enable/disable use of
    // group_by_handler. There is no GBH if SH works for the query.
    if (select_lex->select_h || !get_group_by_handler(thd) || (thd->lex)->sphead)
    {
        return handler;
    }

    // Create a handler if query is valid. See comments for details.
    if ( query->group_by || select_lex->with_sum_func )
    {
        bool unsupported_feature = false;
        // revisit SELECT_LEX for all units
        for(TABLE_LIST* tl = query->from; !unsupported_feature && tl; tl = tl->next_global)
        {
            select_lex = tl->select_lex;
            // Correlation subquery. Comming soon so fail on this yet.
            unsupported_feature = select_lex->is_correlated;

            // Impossible HAVING or WHERE
            if (!unsupported_feature &&
                (select_lex->having_value == Item::COND_FALSE
                  || select_lex->cond_value == Item::COND_FALSE ))
            {
                unsupported_feature = true;
            }

            // Unsupported JOIN conditions
            if ( !unsupported_feature )
            {
                JOIN *join = select_lex->join;
                Item_cond *icp = 0;

                if (join != 0)
                    icp = reinterpret_cast<Item_cond*>(join->conds);

                if (unsupported_feature == false && icp)
                {
                    icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
                }

                // Optimizer could move some join conditions into where
                if (select_lex->where != 0)
                    icp = reinterpret_cast<Item_cond*>(select_lex->where);

                if (unsupported_feature == false && icp)
                {
                    icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
                }
            }

            // Iterate and traverse through the item list and do not create GBH
            // if the unsupported (set/get_user_var) functions are present.
            List_iterator_fast<Item> it(select_lex->item_list);
            Item* item;
            while ((item = it++))
            {
                item_check(item, &unsupported_feature);
                if (unsupported_feature)
                {
                    return handler;
                }
            }
        } // unsupported features check ends here

        if (!unsupported_feature)
        {
            handler = new ha_mcs_group_by_handler(thd, query);

            // Notify the server, that CS handles GROUP BY, ORDER BY and HAVING clauses.
            query->group_by = NULL;
            query->order_by = NULL;
            query->having = NULL;
        }
    }

    return handler;
}

/*@brief  create_columnstore_derived_handler- Creates handler*/
/************************************************************
 * DESCRIPTION:
 * Creates a derived handler if there is no non-equi JOIN, e.g
 * t1.c1 > t2.c2 and logical OR in the filter predicates.
 * More details in server/sql/derived_handler.h
 * PARAMETERS:
 *    thd - THD pointer.
 *    derived - TABLE_LIST* to work with.
 * RETURN:
 *    derived_handler if possible
 *    NULL in other case
 ***********************************************************/
derived_handler*
create_columnstore_derived_handler(THD* thd, TABLE_LIST *derived)
{
    ha_columnstore_derived_handler* handler = NULL;

    // MCOL-2178 Disable SP support in the derived_handler for now
    // Check the session variable value to enable/disable use of
    // derived_handler
    if (!get_derived_handler(thd) || (thd->lex)->sphead)
    {
        return handler;
    }

    SELECT_LEX_UNIT *unit= derived->derived;
    SELECT_LEX *sl= unit->first_select();

    bool unsupported_feature = false;

    // Impossible HAVING or WHERE
    if ( unsupported_feature
       || sl->having_value == Item::COND_FALSE
        || sl->cond_value == Item::COND_FALSE )
    {
        unsupported_feature = true;
    }

    // JOIN expression from WHERE, ON expressions
    JOIN* join= sl->join;
    //TODO DRRTUY Make a proper tree traverse
    //To search for CROSS JOIN-s we use tree invariant
    //G(V,E) where [V] = [E]+1
    List<Item> join_preds_list;
    TABLE_LIST *tl;   
    for (tl = sl->get_table_list(); !unsupported_feature && tl; tl = tl->next_local)
    {
        Item_cond* where_icp= 0;
        Item_cond* on_icp= 0;
        if (tl->where != 0)
        {
            where_icp= reinterpret_cast<Item_cond*>(tl->where);
        }

        if (where_icp)
        {
            where_icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
            where_icp->traverse_cond(save_join_predicates, &join_preds_list, Item::POSTFIX);
        }

        // Looking for JOIN with ON expression through
        // TABLE_LIST in FROM until CS meets unsupported feature
        if (tl->on_expr)
        {
            on_icp= reinterpret_cast<Item_cond*>(tl->on_expr);
            on_icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
            on_icp->traverse_cond(save_join_predicates, &join_preds_list, Item::POSTFIX);
        }

        // Iterate and traverse through the item list and do not create DH
        // if the unsupported (set/get_user_var) functions are present.
        List_iterator_fast<Item> it(tl->select_lex->item_list);
        Item* item;
        while ((item = it++))
        {
            item_check(item, &unsupported_feature);
            if (unsupported_feature)
            {
                return handler;
            }
        }
    }

    if (!unsupported_feature && !join_preds_list.elements
          && join && join->conds)
    {
        Item_cond* conds= reinterpret_cast<Item_cond*>(join->conds);
        conds->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
        conds->traverse_cond(save_join_predicates, &join_preds_list, Item::POSTFIX);
    }

    // CROSS JOIN w/o conditions isn't supported until MCOL-301
    // is ready.
    if (!unsupported_feature && join
         && join->table_count >= 2 && !join_preds_list.elements)
    {
        unsupported_feature= true;
    }

    // CROSS JOIN with not enough JOIN predicates
    if(!unsupported_feature && join
        && join_preds_list.elements < join->table_count-1)
    {
        unsupported_feature= true;
    }

    if ( !unsupported_feature )
        handler= new ha_columnstore_derived_handler(thd, derived);

  return handler;
}

/***********************************************************
 * DESCRIPTION:
 * derived_handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    tbl - tables involved.
 ***********************************************************/
ha_columnstore_derived_handler::ha_columnstore_derived_handler(THD *thd,
                                                             TABLE_LIST *dt)
  : derived_handler(thd, mcs_hton)
{
  derived = dt;
}

/***********************************************************
 * DESCRIPTION:
 * derived_handler destructor
 ***********************************************************/
ha_columnstore_derived_handler::~ha_columnstore_derived_handler()
{}

/*@brief  Initiate the query for derived_handler           */
/***********************************************************
 * DESCRIPTION:
 * Execute the query and saves derived table query.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 ***********************************************************/
int ha_columnstore_derived_handler::init_scan()
{
    DBUG_ENTER("ha_columnstore_derived_handler::init_scan");

    mcs_handler_info mhi = mcs_handler_info(reinterpret_cast<void*>(this), DERIVED);
    // this::table is the place for the result set
    int rc = ha_cs_impl_pushdown_init(&mhi, table);

    DBUG_RETURN(rc);
}

/*@brief  Fetch next row for derived_handler               */
/***********************************************************
 * DESCRIPTION:
 * Fetches next row and saves it in the temp table
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_derived_handler::next_row()
{
    DBUG_ENTER("ha_columnstore_derived_handler::next_row");

    int rc = ha_mcs_impl_rnd_next(table->record[0], table);

    DBUG_RETURN(rc);
}

/*@brief  Finishes the scan and clean it up               */
/***********************************************************
 * DESCRIPTION:
 * Finishes the scan for derived handler
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_derived_handler::end_scan()
{
    DBUG_ENTER("ha_columnstore_derived_handler::end_scan");

    int rc = ha_mcs_impl_rnd_end(table, true);

    DBUG_RETURN(rc);
}

void ha_columnstore_derived_handler::print_error(int, unsigned long)
{
}

/***********************************************************
 * DESCRIPTION:
 * GROUP BY handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    query - Query describing structure
 ***********************************************************/
ha_mcs_group_by_handler::ha_mcs_group_by_handler(THD* thd_arg, Query* query)
        : group_by_handler(thd_arg, mcs_hton),
          select(query->select),
          table_list(query->from),
          distinct(query->distinct),
          where(query->where),
          group_by(query->group_by),
          order_by(query->order_by),
          having(query->having)
{
}

/***********************************************************
 * DESCRIPTION:
 * GROUP BY destructor
 ***********************************************************/
ha_mcs_group_by_handler::~ha_mcs_group_by_handler()
{
}

/***********************************************************
 * DESCRIPTION:
 * Makes the plan and prepares the data
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_mcs_group_by_handler::init_scan()
{
    DBUG_ENTER("ha_mcs_group_by_handler::init_scan");

    mcs_handler_info mhi = mcs_handler_info(reinterpret_cast<void*>(this), GROUP_BY);
    int rc = ha_mcs_impl_group_by_init(&mhi, table);

    DBUG_RETURN(rc);
}

/***********************************************************
 * DESCRIPTION:
 * Fetches a row and saves it to a temporary table.
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_mcs_group_by_handler::next_row()
{
    DBUG_ENTER("ha_mcs_group_by_handler::next_row");
    int rc = ha_mcs_impl_group_by_next(table);

    DBUG_RETURN(rc);
}

/***********************************************************
 * DESCRIPTION:
 * Shuts the scan down.
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_mcs_group_by_handler::end_scan()
{
    DBUG_ENTER("ha_mcs_group_by_handler::end_scan");
    int rc = ha_mcs_impl_group_by_end(table);

    DBUG_RETURN(rc);
}

/*@brief  create_columnstore_select_handler- Creates handler*/
/************************************************************
 * DESCRIPTION:
 * Creates a select handler if there is no non-equi JOIN, e.g
 * t1.c1 > t2.c2 and logical OR in the filter predicates.
 * More details in server/sql/select_handler.h
 * PARAMETERS:
 *    thd - THD pointer.
 *    sel - SELECT_LEX* that describes the query.
 * RETURN:
 *    select_handler if possible
 *    NULL in other case
 ***********************************************************/
select_handler*
create_columnstore_select_handler(THD* thd, SELECT_LEX* select_lex)
{
    ha_columnstore_select_handler* handler = NULL;

    // MCOL-2178 Disable SP support in the select_handler for now.
    // Check the session variable value to enable/disable use of
    // select_handler
    // Disable processing of select_result_interceptor classes
    // which intercept and transform result set rows. E.g.:
    // select a,b into @a1, @a2 from t1;
    if (!get_select_handler(thd) || (thd->lex)->sphead ||
        ((thd->lex)->result &&
         !((select_dumpvar *)(thd->lex)->result)->var_list.is_empty()))
    {
        return handler;
    }

    // Save the original group_list as it can be mutated by the
    // optimizer which calls the remove_const() function
    Group_list_ptrs *group_list_ptrs = NULL;
    if (save_group_list(thd, select_lex, &group_list_ptrs))
    {
        return handler;
    }

    bool unsupported_feature = false;
    // Select_handler use the short-cut that effectively disables
    // INSERT..SELECT, LDI, SELECT..INTO OUTFILE
    if ((thd->lex)->sql_command == SQLCOM_INSERT_SELECT
        || (thd->lex)->sql_command == SQLCOM_CREATE_TABLE
        || (thd->lex)->exchange)
        
    {
        unsupported_feature = true;
    }

    JOIN *join= select_lex->join;
    // WIP
    COND *original_where = join->conds;
    // Next block tries to execute the query using SH very early to fallback
    // if execution fails.
    if (!unsupported_feature)
    {
        // Most of optimizer_switch flags disabled in external_lock
        //join->optimization_state= JOIN::OPTIMIZATION_IN_PROGRESS;
        //join->optimize_inner();
        // WIP
        COND *conds = simplify_joins_(join, select_lex->join_list, join->conds, TRUE, FALSE);
    
        // WIP
        if (conds)
        {
#ifdef DEBUG_WALK_COND
            conds->traverse_cond(cal_impl_if::debug_walk, NULL, Item::POSTFIX);
#endif
            join->conds = conds;
        }

        // Impossible HAVING or WHERE
        // TODO replace with function call
        if (unsupported_feature
           || select_lex->having_value == Item::COND_FALSE
            || select_lex->cond_value == Item::COND_FALSE )
        {
            unsupported_feature = true;
            restore_optimizer_flags(thd);
        }
    }

    // Restore back the saved group_list
    if (group_list_ptrs)
    {
        restore_group_list(select_lex, group_list_ptrs);
    }

    // Iterate and traverse through the item list and do not create SH
    // if the unsupported (set/get_user_var) functions are present.
    TABLE_LIST* table_ptr = select_lex->get_table_list();
    for (; !unsupported_feature && table_ptr; table_ptr = table_ptr->next_global)
    {
        List_iterator_fast<Item> it(table_ptr->select_lex->item_list);
        Item* item;
        while ((item = it++))
        {
            item_check(item, &unsupported_feature);
            if (unsupported_feature)
            {
                break;
            }
        }
    }


    if (!unsupported_feature)
    {
        handler= new ha_columnstore_select_handler(thd, select_lex);
        // This is an ugly hack to call simplify_joins()
        mcs_handler_info mhi= mcs_handler_info(reinterpret_cast<void*>(handler), SELECT);
        // this::table is the place for the result set
        int rc= ha_cs_impl_pushdown_init(&mhi, handler->table);

        // WIP check the xpression
        // Return SH if query execution is fine or fallback is disabled
        if (!rc || !get_fallback_knob(thd))
        {
            join->conds = original_where;
            return handler;
        }

        // Reset the DA and restore optimizer flags
        // to allow query to fallback to other handlers
        if (thd->get_stmt_da()->is_error())
        {
            thd->get_stmt_da()->reset_diagnostics_area();
            restore_optimizer_flags(thd);
            unsupported_feature = true;
        }
        // WIP
        join->conds = original_where;
    }

    if (join->optimization_state != JOIN::NOT_OPTIMIZED)
    {
        if (!join->with_two_phase_optimization)
        {
            if (unsupported_feature && join->have_query_plan != JOIN::QEP_DELETED)
            {
                join->build_explain();
            }
            join->optimization_state= JOIN::OPTIMIZATION_DONE;
        }
        else
        {
            join->optimization_state= JOIN::OPTIMIZATION_PHASE_1_DONE;
        }
    }

    return NULL;
}

/***********************************************************
 * DESCRIPTION:
 * select_handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    select_lex - sematic tree for the query.
 ***********************************************************/
ha_columnstore_select_handler::ha_columnstore_select_handler(THD *thd,
                                                             SELECT_LEX* select_lex)
  : select_handler(thd, mcs_hton)
{
  select = select_lex;
}

/***********************************************************
 * DESCRIPTION:
 * select_handler constructor
 ***********************************************************/
ha_columnstore_select_handler::~ha_columnstore_select_handler()
{
}

/*@brief  Initiate the query for select_handler           */
/***********************************************************
 * DESCRIPTION:
 * Execute the query and saves select table query.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 ***********************************************************/
int ha_columnstore_select_handler::init_scan()
{
    DBUG_ENTER("ha_columnstore_select_handler::init_scan");

    // Dummy init for SH. Actual init happens in create_SH
    // to allow fallback to other handlers if SH fails.
    int rc = 0;

    DBUG_RETURN(rc);
}

/*@brief  Fetch next row for select_handler               */
/***********************************************************
 * DESCRIPTION:
 * Fetches next row and saves it in the temp table
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_select_handler::next_row()
{
    DBUG_ENTER("ha_columnstore_select_handler::next_row");

    int rc= ha_cs_impl_select_next(table->record[0], table);

    DBUG_RETURN(rc);
}

/*@brief  Finishes the scan and clean it up               */
/***********************************************************
 * DESCRIPTION:
 * Finishes the scan for select handler
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_select_handler::end_scan()
{
    DBUG_ENTER("ha_columnstore_select_handler::end_scan");

    int rc = ha_mcs_impl_rnd_end(table, true);

    DBUG_RETURN(rc);
}

void ha_columnstore_select_handler::print_error(int, unsigned long)
{}

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

