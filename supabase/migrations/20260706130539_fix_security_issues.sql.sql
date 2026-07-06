-- Enable RLS on Conversation_History table
ALTER TABLE "Conversation_History" ENABLE ROW LEVEL SECURITY;

-- Create RLS policies for Conversation_History
-- Since this appears to be a no-auth app (uses session IDs), allow both anon and authenticated
CREATE POLICY "select_conversation_history" ON "Conversation_History" FOR SELECT
  TO anon, authenticated USING (true);

CREATE POLICY "insert_conversation_history" ON "Conversation_History" FOR INSERT
  TO anon, authenticated WITH CHECK (true);

CREATE POLICY "update_conversation_history" ON "Conversation_History" FOR UPDATE
  TO anon, authenticated USING (true) WITH CHECK (true);

CREATE POLICY "delete_conversation_history" ON "Conversation_History" FOR DELETE
  TO anon, authenticated USING (true);

-- Revoke execute permissions on rls_auto_enable from anon and authenticated
-- This function should only be executable by superusers/postgres role
REVOKE EXECUTE ON FUNCTION public.rls_auto_enable() FROM anon, authenticated;