-- Allow a Supabase Auth user to be deleted without deleting shared research data.
-- Run once in Supabase SQL Editor. This does not delete any rows.

begin;

alter table public.family_groups
  alter column created_by drop not null;

alter table public.family_groups
  drop constraint if exists family_groups_created_by_fkey;

alter table public.family_groups
  add constraint family_groups_created_by_fkey
  foreign key (created_by) references auth.users(id) on delete set null;

alter table public.watchlist_items
  alter column added_by drop not null;

alter table public.watchlist_items
  drop constraint if exists watchlist_items_added_by_fkey;

alter table public.watchlist_items
  add constraint watchlist_items_added_by_fkey
  foreign key (added_by) references auth.users(id) on delete set null;

commit;
