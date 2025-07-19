/* include/pg_tkach_scheduler.h */

#ifndef PG_TKACH_SCHEDULER
#define PG_TKACH_SCHEDULER

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

Datum ts_schedule(PG_FUNCTION_ARGS);
Datum ts_unschedule(PG_FUNCTION_ARGS);

static bool isValidQuery(const char *);

#endif