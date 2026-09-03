create table if not exists public.source_customers_cdc_demo (
    customer_id text primary key,
    external_account_id text not null,
    email text,
    phone text,
    first_name text,
    last_name text,
    tenant_id text not null,
    business_unit text not null,
    customer_status text not null,
    created_at timestamptz,
    updated_at timestamptz not null,
    source_updated_at timestamptz
);

alter table public.source_customers_cdc_demo replica identity full;

-- The core six-domain publication is created before this bounded proof table.
-- Add the proof table explicitly so the connector include list and publication
-- remain aligned on a fresh Compose initialization.
do $$
begin
    if not exists (
        select 1 from pg_publication_tables
        where pubname = 'customer360_publication'
          and schemaname = 'public'
          and tablename = 'source_customers_cdc_demo'
    ) then
        alter publication customer360_publication add table public.source_customers_cdc_demo;
    end if;
exception
    when undefined_object then
        null;
end $$;

insert into public.source_customers_cdc_demo (
    customer_id,
    external_account_id,
    email,
    phone,
    first_name,
    last_name,
    tenant_id,
    business_unit,
    customer_status,
    created_at,
    updated_at,
    source_updated_at
)
values (
    'cdc_demo_customer_001',
    'acct_ext_cdc_demo_001',
    'cdc.demo@example.com',
    '+14155551010',
    'Cdc',
    'Demo',
    'tenant_us',
    'self_serve',
    'lead',
    '2026-06-01T00:00:00Z',
    '2026-06-01T00:00:00Z',
    '2026-06-01T00:00:00Z'
)
on conflict (customer_id) do update set
    customer_status = excluded.customer_status,
    updated_at = excluded.updated_at,
    source_updated_at = excluded.source_updated_at;

update public.source_customers_cdc_demo
set
    customer_status = 'active',
    updated_at = '2026-06-01T00:05:00Z',
    source_updated_at = '2026-06-01T00:05:00Z'
where customer_id = 'cdc_demo_customer_001';

delete from public.source_customers_cdc_demo
where customer_id = 'cdc_demo_customer_001';
