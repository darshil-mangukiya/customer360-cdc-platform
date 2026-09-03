-- Tenant RLS uses fixed database-role bindings. The tenant is derived from the
-- authenticated session_user; clients cannot choose it with SET/SET LOCAL.
do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'p7_tenant_us_app') then
        create role p7_tenant_us_app noinherit nologin;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'p7_tenant_eu_app') then
        create role p7_tenant_eu_app noinherit nologin;
    end if;
end $$;

create table if not exists audit.tenant_database_role (
    database_role name primary key,
    tenant_id text not null unique,
    constraint tenant_database_role_is_current_role
    check (database_role in ('p7_tenant_us_app', 'p7_tenant_eu_app'))
);

insert into audit.tenant_database_role (database_role, tenant_id)
values
('p7_tenant_us_app', 'tenant_us'),
('p7_tenant_eu_app', 'tenant_eu')
on conflict (database_role) do update set tenant_id = excluded.tenant_id;

create or replace function audit.authenticated_tenant_id()
returns text
language sql
stable
security definer
set search_path = pg_catalog, audit
as $$
    select tenant_id
    from audit.tenant_database_role
    where database_role = session_user::name
$$;

revoke all on function audit.authenticated_tenant_id() from public;
grant usage on schema audit, identity, activation to p7_tenant_us_app, p7_tenant_eu_app;
grant execute on function audit.authenticated_tenant_id() to p7_tenant_us_app, p7_tenant_eu_app;
grant select, insert, update, delete on identity.identity_review_queue to p7_tenant_us_app, p7_tenant_eu_app;
grant select on identity.dim_customer_canonical to p7_tenant_us_app, p7_tenant_eu_app;
grant select on activation.export_customer_segment to p7_tenant_us_app, p7_tenant_eu_app;

alter table identity.identity_review_queue enable row level security;
alter table identity.dim_customer_canonical enable row level security;
alter table activation.export_customer_segment enable row level security;

drop policy if exists tenant_review_queue_isolation on identity.identity_review_queue;
create policy tenant_review_queue_isolation
on identity.identity_review_queue
for all
to p7_tenant_us_app, p7_tenant_eu_app
using (tenant_id = audit.authenticated_tenant_id())
with check (tenant_id = audit.authenticated_tenant_id());

drop policy if exists tenant_customer_360_identity_visibility on identity.dim_customer_canonical;
create policy tenant_customer_360_identity_visibility
on identity.dim_customer_canonical
for select
to p7_tenant_us_app, p7_tenant_eu_app
using (tenant_id = audit.authenticated_tenant_id());

drop policy if exists tenant_activation_output_visibility on activation.export_customer_segment;
create policy tenant_activation_output_visibility
on activation.export_customer_segment
for select
to p7_tenant_us_app, p7_tenant_eu_app
using (tenant_id = audit.authenticated_tenant_id());
