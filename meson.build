pg_plan_watch_sources = files(
  'pg_plan_watch.c',
)

if host_system == 'windows'
  pg_plan_watch_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_plan_watch',
    '--FILEDESC', 'pg_plan_watch - logging facility for execution plans',])
endif

pg_plan_watch = shared_module('pg_plan_watch',
  pg_plan_watch_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_plan_watch