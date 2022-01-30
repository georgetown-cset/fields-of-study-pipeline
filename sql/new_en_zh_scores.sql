select merged_id, fields from {{staging_dataset}}.new_en
union all
select merged_id, fields from {{staging_dataset}}.new_zh