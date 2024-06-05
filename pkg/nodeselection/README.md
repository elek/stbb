

Query to export nodes:

```SQL
select * from nodes 
WHERE disqualified IS NULL
AND unknown_audit_suspended IS NULL
AND offline_suspended IS NULL
AND exit_initiated_at IS NULL
AND free_disk >= 5000000000
AND last_contact_success > current_timestamp() - INTERVAL '4 hours'
```

performance data:
```SQL
SELECT tag_instance,tag_field, min(tag_value) as minimum,max(tag_value) as maximum, avg(tag_value) as average
FROM `storj-data-science-249814.eventkitd3.storj_io_statreceiver` WHERE 
TIMESTAMP_TRUNC(received_at, DAY) > TIMESTAMP("2024-06-01") 
AND tag_name = 'download_success_duration_ns'
AND tag_field != 'count'
group by tag_field,tag_name,tag_instance
order by tag_field,tag_name, tag_instance

```