# contrib/pg_plan_watch/Makefile

MODULE_big = pg_plan_watch
OBJS = \
	$(WIN32RES) \
	pg_plan_watch.o
PGFILEDESC = "pg_plan_watch - logging facility for execution plans"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_plan_watch
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
