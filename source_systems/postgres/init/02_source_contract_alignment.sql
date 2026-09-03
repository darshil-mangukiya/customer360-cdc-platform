-- Idempotent compatibility migration for local volumes created before P7 2.0.
-- Defaults only protect pre-existing synthetic fixture rows; the bounded seed
-- immediately replaces them with their actual tenant/domain values.
alter table public.subscriptions add column if not exists tenant_id text default 'tenant_unknown';
alter table public.subscriptions add column if not exists business_unit text default 'unknown';
alter table public.orders add column if not exists tenant_id text default 'tenant_unknown';
alter table public.orders add column if not exists business_unit text default 'unknown';
alter table public.engagement_events add column if not exists tenant_id text default 'tenant_unknown';
alter table public.engagement_events add column if not exists business_unit text default 'unknown';
alter table public.support_interactions add column if not exists tenant_id text default 'tenant_unknown';
alter table public.support_interactions add column if not exists business_unit text default 'unknown';
alter table public.marketing_engagement add column if not exists tenant_id text default 'tenant_unknown';
alter table public.marketing_engagement add column if not exists business_unit text default 'unknown';
alter table public.marketing_engagement add column if not exists marketing_consent_status text default 'unknown';
alter table public.marketing_engagement add column if not exists email_opt_in boolean default false;
alter table public.marketing_engagement add column if not exists sms_opt_in boolean default false;
alter table public.marketing_engagement add column if not exists push_opt_in boolean default false;
alter table public.marketing_engagement add column if not exists unsubscribe_status text default 'unsubscribed';
alter table public.marketing_engagement add column if not exists do_not_contact_flag boolean default true;

alter table public.subscriptions alter column tenant_id set not null;
alter table public.subscriptions alter column business_unit set not null;
alter table public.orders alter column tenant_id set not null;
alter table public.orders alter column business_unit set not null;
alter table public.engagement_events alter column tenant_id set not null;
alter table public.engagement_events alter column business_unit set not null;
alter table public.support_interactions alter column tenant_id set not null;
alter table public.support_interactions alter column business_unit set not null;
alter table public.marketing_engagement alter column tenant_id set not null;
alter table public.marketing_engagement alter column business_unit set not null;
alter table public.marketing_engagement alter column marketing_consent_status set not null;
alter table public.marketing_engagement alter column email_opt_in set not null;
alter table public.marketing_engagement alter column sms_opt_in set not null;
alter table public.marketing_engagement alter column push_opt_in set not null;
alter table public.marketing_engagement alter column unsubscribe_status set not null;
alter table public.marketing_engagement alter column do_not_contact_flag set not null;

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
end $$;
