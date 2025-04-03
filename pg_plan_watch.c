/*-------------------------------------------------------------------------
 *
 * pg_plan_watch.c
 *
 * IDENTIFICATION
 *	  contrib/pg_plan_watch/pg_plan_watch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/parallel.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "common/pg_prng.h"
#include "executor/instrument.h"
#include "utils/guc.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_plan_watch",
					.version = PG_VERSION
);

/* GUC variables */
static int	pg_plan_watch_log_min_duration = -1; /* msec or -1 */
static int	pg_plan_watch_log_parameter_max_length = -1; /* bytes or -1 */
static bool pg_plan_watch_log_analyze = false;
static bool pg_plan_watch_log_verbose = false;
static bool pg_plan_watch_log_buffers = false;
static bool pg_plan_watch_log_wal = false;
static bool pg_plan_watch_log_triggers = false;
static bool pg_plan_watch_log_timing = true;
static bool pg_plan_watch_log_settings = false;
static int	pg_plan_watch_log_format = EXPLAIN_FORMAT_TEXT;
static int	pg_plan_watch_log_level = LOG;
static bool pg_plan_watch_log_nested_statements = false;

static const struct config_enum_entry format_options[] = {
	{"text", EXPLAIN_FORMAT_TEXT, false},
	{"xml", EXPLAIN_FORMAT_XML, false},
	{"json", EXPLAIN_FORMAT_JSON, false},
	{"yaml", EXPLAIN_FORMAT_YAML, false},
	{NULL, 0, false}
};

static const struct config_enum_entry loglevel_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"log", LOG, false},
	{NULL, 0, false}
};

/* Current nesting depth of ExecutorRun calls */
static int	nesting_level = 0;

#define pg_plan_watch_enabled() \
	(pg_plan_watch_log_min_duration >= 0 && \
	 (nesting_level == 0 || pg_plan_watch_log_nested_statements))

/* Saved hook values */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static bool explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
								ScanDirection direction,
								uint64 count);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomIntVariable("pg_plan_watch.log_min_duration",
							"Sets the minimum execution time above which plans will be logged.",
							"-1 disables logging plans. 0 means log all plans.",
							&pg_plan_watch_log_min_duration,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_plan_watch.log_parameter_max_length",
							"Sets the maximum length of query parameter values to log.",
							"-1 means log values in full.",
							&pg_plan_watch_log_parameter_max_length,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_BYTE,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_analyze",
							 "Use EXPLAIN ANALYZE for plan logging.",
							 NULL,
							 &pg_plan_watch_log_analyze,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_settings",
							 "Log modified configuration parameters affecting query planning.",
							 NULL,
							 &pg_plan_watch_log_settings,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_verbose",
							 "Use EXPLAIN VERBOSE for plan logging.",
							 NULL,
							 &pg_plan_watch_log_verbose,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_buffers",
							 "Log buffers usage.",
							 NULL,
							 &pg_plan_watch_log_buffers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_wal",
							 "Log WAL usage.",
							 NULL,
							 &pg_plan_watch_log_wal,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_triggers",
							 "Include trigger statistics in plans.",
							 "This has no effect unless log_analyze is also set.",
							 &pg_plan_watch_log_triggers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_plan_watch.log_format",
							 "EXPLAIN format to be used for plan logging.",
							 NULL,
							 &pg_plan_watch_log_format,
							 EXPLAIN_FORMAT_TEXT,
							 format_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_plan_watch.log_level",
							 "Log level for the plan.",
							 NULL,
							 &pg_plan_watch_log_level,
							 LOG,
							 loglevel_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_nested_statements",
							 "Log nested statements.",
							 NULL,
							 &pg_plan_watch_log_nested_statements,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_plan_watch.log_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &pg_plan_watch_log_timing,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_plan_watch");

	/* Install hooks. */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static bool
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	bool		plan_valid;

	if (pg_plan_watch_enabled())
	{
		/* Enable per-node instrumentation iff log_analyze is required. */
		if (pg_plan_watch_log_analyze && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		{
			if (pg_plan_watch_log_timing)
				queryDesc->instrument_options |= INSTRUMENT_TIMER;
			else
				queryDesc->instrument_options |= INSTRUMENT_ROWS;
			if (pg_plan_watch_log_buffers)
				queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
			if (pg_plan_watch_log_wal)
				queryDesc->instrument_options |= INSTRUMENT_WAL;
		}
	}

	if (prev_ExecutorStart)
		plan_valid = prev_ExecutorStart(queryDesc, eflags);
	else
		plan_valid = standard_ExecutorStart(queryDesc, eflags);

	/* The plan may have become invalid during standard_ExecutorStart() */
	if (!plan_valid)
		return false;

	if (pg_plan_watch_enabled())
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	return true;
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && pg_plan_watch_enabled())
	{
		MemoryContext oldcxt;

		/*
		 * Make sure we operate in the per-query context, so any cruft will be
		 * discarded later during ExecutorEnd.
		 */
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		if (true)
		{
			ExplainState *es = NewExplainState();

			es->analyze = (queryDesc->instrument_options && pg_plan_watch_log_analyze);
			es->verbose = pg_plan_watch_log_verbose;
			es->buffers = (es->analyze && pg_plan_watch_log_buffers);
			es->wal = (es->analyze && pg_plan_watch_log_wal);
			es->timing = (es->analyze && pg_plan_watch_log_timing);
			es->summary = es->analyze;
			/* No support for MEMORY */
			/* es->memory = false; */
			es->format = pg_plan_watch_log_format;
			es->settings = pg_plan_watch_log_settings;

			ExplainBeginOutput(es);
			ExplainQueryText(es, queryDesc);
			ExplainQueryParameters(es, queryDesc->params, pg_plan_watch_log_parameter_max_length);
			ExplainPrintPlan(es, queryDesc);
			if (es->analyze && pg_plan_watch_log_triggers)
				ExplainPrintTriggers(es, queryDesc);
			if (es->costs)
				ExplainPrintJITSummary(es, queryDesc);
			ExplainEndOutput(es);

			/* Remove last line break */
			if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
				es->str->data[--es->str->len] = '\0';

			/* Fix JSON to output an object */
			if (pg_plan_watch_log_format == EXPLAIN_FORMAT_JSON)
			{
				es->str->data[0] = '{';
				es->str->data[es->str->len - 1] = '}';
			}

			/*
			 * Note: we rely on the existing logging of context or
			 * debug_query_string to identify just which statement is being
			 * reported.  This isn't ideal but trying to do it here would
			 * often result in duplication.
			 */
			ereport(pg_plan_watch_log_level,
					(errmsg("duration: %.3f ms  plan:\n%s",
							queryDesc->totaltime->total * 1000.0, es->str->data),
					 errhidestmt(true)));
		}

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}
