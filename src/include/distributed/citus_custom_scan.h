/*-------------------------------------------------------------------------
 *
 * citus_custom_scan.h
 *	  Export all custom scan and custom exec methods.
 *
 * Copyright (c) 2012-2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CUSTOM_SCAN_H
#define CITUS_CUSTOM_SCAN_H

#include "distributed/distributed_planner.h"
#include "distributed/multi_server_executor.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"


typedef struct CitusScanState
{
	CustomScanState customScanState;  /* underlying custom scan node */
	DistributedPlan *distributedPlan; /* distributed execution plan */
	MultiExecutorType executorType;   /* distributed executor type */
	bool finishedRemoteScan;          /* flag to check if remote scan is finished */
	Tuplestorestate *tuplestorestate; /* tuple store to store distributed results */
} CitusScanState;


/* custom scan methods for all executors */
extern CustomScanMethods RealTimeCustomScanMethods;
extern CustomScanMethods TaskTrackerCustomScanMethods;
extern CustomScanMethods RouterCustomScanMethods;
extern CustomScanMethods CoordinatorInsertSelectCustomScanMethods;
extern CustomScanMethods DelayedErrorCustomScanMethods;


extern void RegisterCitusCustomScanMethods(void);
extern void CitusExplainScan(CustomScanState *node, List *ancestors, struct
							 ExplainState *es);
extern TupleDesc ScanStateGetTupleDescriptor(CitusScanState *scanState);
extern EState * ScanStateGetExecutorState(CitusScanState *scanState);

#endif /* CITUS_CUSTOM_SCAN_H */
