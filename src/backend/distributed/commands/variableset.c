/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *    Commands for vacuuming distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "common/string.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "lib/ilist.h"
#include "distributed/remote_commands.h"


static void StartRemoteTransactionVariableModify(MultiConnection *connection, const
												 char *setCommand);
static void FinishRemoteTransactionVariableModify(MultiConnection *connection);


/*
 * TODO: document
 */
void
ProcessVariableSetStmt(VariableSetStmt *setStmt, const char *setCommand)
{
	dlist_iter iter;
	const bool raiseInterrupts = true;
	List *connectionList = NIL;

	/* only perform the following if we appear to be the coordinator for a txn */
	if (activeSetStmts == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(CurTransactionContext);
		activeSetStmts = makeStringInfo();
		MemoryContextSwitchTo(old_context);
	}

	/* asynchronously send SAVEPOINT */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		if (transaction->transactionFailed)
		{
			continue;
		}

		StartRemoteTransactionVariableModify(connection, setCommand);
		connectionList = lappend(connectionList, connection);
	}

	WaitForAllConnections(connectionList, raiseInterrupts);

	/* and wait for the results */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		FinishRemoteTransactionVariableModify(connection);
	}

	appendStringInfoString(activeSetStmts, setCommand);

	if (!pg_str_endswith(setCommand, ";"))
	{
		appendStringInfoChar(activeSetStmts, ';');
	}
}


/*
 * TODO: document
 */
static void
StartRemoteTransactionVariableModify(MultiConnection *connection, const char *setCommand)
{
	const bool raiseErrors = true;

	if (!SendRemoteCommand(connection, setCommand))
	{
		HandleRemoteTransactionConnectionError(connection, raiseErrors);
	}
}


/*
 * TODO: document
 */
static void
FinishRemoteTransactionVariableModify(MultiConnection *connection)
{
	const bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
	if (!IsResponseOK(result))
	{
		HandleRemoteTransactionResultError(connection, result, raiseErrors);
	}

	PQclear(result);
	ForgetResults(connection);
}
