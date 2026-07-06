-- Revoke execute from PUBLIC (covers anon, authenticated, and all future roles)
REVOKE EXECUTE ON FUNCTION public.rls_auto_enable() FROM PUBLIC;