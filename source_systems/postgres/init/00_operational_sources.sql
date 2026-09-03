create table if not exists public.customers (
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

create table if not exists public.subscriptions (
    subscription_id text primary key,
    tenant_id text not null,
    business_unit text not null,
    customer_id text,
    external_account_id text not null,
    email text,
    plan_name text not null,
    subscription_status text not null,
    billing_period text not null,
    mrr numeric(14, 2) not null,
    start_date timestamptz,
    trial_end_date timestamptz,
    cancel_at timestamptz,
    updated_at timestamptz not null
);

create table if not exists public.orders (
    order_id text primary key,
    tenant_id text not null,
    business_unit text not null,
    order_customer_ref text not null,
    email text,
    subscription_id text not null,
    order_status text not null,
    gross_amount numeric(14, 2) not null,
    currency text not null,
    ordered_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists public.engagement_events (
    engagement_event_id text primary key,
    tenant_id text not null,
    business_unit text not null,
    device_id text not null,
    customer_id text,
    email text,
    event_name text not null,
    event_count int not null,
    session_minutes int not null,
    event_timestamp timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists public.support_interactions (
    support_interaction_id text primary key,
    tenant_id text not null,
    business_unit text not null,
    support_customer_ref text not null,
    email text,
    phone text,
    reason text not null,
    priority text not null,
    status text not null,
    csat_score int,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists public.marketing_engagement (
    marketing_touch_id text primary key,
    tenant_id text not null,
    business_unit text not null,
    email text not null,
    external_account_id text,
    channel text not null,
    campaign_id text not null,
    engagement_status text not null,
    marketing_consent_status text not null,
    email_opt_in boolean not null,
    sms_opt_in boolean not null,
    push_opt_in boolean not null,
    unsubscribe_status text not null,
    do_not_contact_flag boolean not null,
    lead_score int not null,
    occurred_at timestamptz not null,
    updated_at timestamptz not null
);

create table if not exists public.cdc_heartbeat (
    ts timestamptz not null
);

alter table public.customers replica identity full;
alter table public.subscriptions replica identity full;
alter table public.orders replica identity full;
alter table public.engagement_events replica identity full;
alter table public.support_interactions replica identity full;
alter table public.marketing_engagement replica identity full;

drop publication if exists customer360_publication;
create publication customer360_publication for table
public.customers,
public.subscriptions,
public.orders,
public.engagement_events,
public.support_interactions,
public.marketing_engagement;
