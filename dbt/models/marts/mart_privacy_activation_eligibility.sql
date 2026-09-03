{{ config(materialized='table') }}

with mapped_consent_events as (
    select
        m.canonical_customer_id,
        s.tenant_id,
        s.marketing_consent_status,
        s.email_opt_in,
        s.sms_opt_in,
        s.push_opt_in,
        s.unsubscribe_status,
        s.do_not_contact_flag,
        s.is_delete as consent_source_deleted,
        s.event_timestamp as consent_event_at,
        s.event_id as consent_event_id,
        row_number() over (
            partition by s.tenant_id, m.canonical_customer_id
            order by s.event_sequence_number desc, s.event_timestamp desc, s.event_id desc
        ) as consent_rank
    from {{ ref('stg_marketing_engagement') }} s
    inner join {{ source('identity', 'customer_identity_map') }} m
        on s.tenant_id = m.tenant_id
        and s.source_marketing_touch_id = m.source_record_id
        and m.source_table = 'marketing_engagement'
        and m.is_active
),
current_consent as (
    select *
    from mapped_consent_events
    where consent_rank = 1
),
deletion_flags as (
    select
        canonical_customer_id,
        max(case when tenant_id is null then 1 else 0 end) as has_global_deletion,
        max(case when tenant_id is not null then 1 else 0 end) as has_tenant_deletion
    from {{ source('privacy', 'deletion_request') }}
    group by 1
),
base as (
    select
        c.canonical_customer_id,
        c.tenant_id,
        consent.marketing_consent_status,
        coalesce(consent.email_opt_in, false) as email_opt_in,
        coalesce(consent.sms_opt_in, false) as sms_opt_in,
        coalesce(consent.push_opt_in, false) as push_opt_in,
        consent.unsubscribe_status,
        coalesce(consent.do_not_contact_flag, false) as do_not_contact,
        coalesce(consent.consent_source_deleted, false) as consent_source_deleted,
        consent.consent_event_at,
        consent.consent_event_id,
        case
            when deletion.canonical_customer_id is not null then true
            else false
        end as deletion_requested,
        case when c.primary_email is not null or c.primary_phone is not null then true else false end as has_contact_identifier
    from {{ ref('mart_customer_360_current') }} c
    left join current_consent consent
        on c.tenant_id = consent.tenant_id
        and c.canonical_customer_id = consent.canonical_customer_id
    left join deletion_flags deletion
        on c.canonical_customer_id = deletion.canonical_customer_id
)

select
    canonical_customer_id,
    tenant_id,
    marketing_consent_status,
    email_opt_in,
    sms_opt_in,
    push_opt_in,
    unsubscribe_status,
    do_not_contact,
    deletion_requested,
    consent_source_deleted,
    consent_event_at,
    consent_event_id,
    case
        when deletion_requested then false
        when do_not_contact then false
        when consent_source_deleted then false
        when unsubscribe_status = 'unsubscribed' then false
        when coalesce(marketing_consent_status, 'unknown') != 'opted_in' then false
        when not (email_opt_in or sms_opt_in or push_opt_in) then false
        when not has_contact_identifier then false
        else true
    end as activation_eligible,
    case
        when deletion_requested then 'privacy_deletion_requested'
        when do_not_contact then 'do_not_contact'
        when consent_source_deleted then 'consent_source_deleted'
        when unsubscribe_status = 'unsubscribed' then 'unsubscribed'
        when marketing_consent_status is null then 'missing_consent_state'
        when marketing_consent_status != 'opted_in' then 'marketing_consent_' || marketing_consent_status
        when not (email_opt_in or sms_opt_in or push_opt_in) then 'no_active_channel_consent'
        when not has_contact_identifier then 'missing_activation_identifier'
        else null
    end as suppression_reason
from base
