-- pg_partman 일별 파티션 유지보수 쿼리

CALL partman.run_maintenance_proc(p_wait := 0, p_analyze := false, p_jobmon := false);