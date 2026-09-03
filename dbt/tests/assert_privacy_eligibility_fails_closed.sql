select *
from {{ ref('mart_privacy_activation_eligibility') }}
where activation_eligible
  and (
      marketing_consent_status is null
      or marketing_consent_status != 'opted_in'
      or not (email_opt_in or sms_opt_in or push_opt_in)
      or do_not_contact
      or deletion_requested
      or consent_source_deleted
  )
