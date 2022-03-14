with sample as (
  -- Take e.g. records where the (unsigned) remainder of the hash / 100 is less than 10
  -- to get a ~10% sample because ~10% of the hashes should be divisible by 100
  select merged_id
  from field_model_replication.sampling_frame
  where mod(abs(id_hash), 100000) < 1
),

--get level 0 and level 1 field ids
level0_1 AS(SELECT field_id FROM `fields_of_study.field_meta`
                WHERE level = 0 or level = 1),

--{for the merged_ids above, get the fields we want, like title + abstract text + labels, from the staging_ dataset}

select_mag_fields AS(select merged_id, f.id, field_names.name, f.score FROM fields_of_study.field_scores
cross join unnest(fields) as f
inner join sample using(merged_id)
inner join level0_1 on field_id = f.id
left join fields_of_study.field_meta field_names on field_names.field_id = f.id
where is_imputed is false) --we only want articles where the mag model produced a score

select merged_id, array_agg(
    struct(id, name, score)
    ) as fields, title_english, abstract_english from select_mag_fields
left join gcp_cset_links_v2.corpus_merged using(merged_id)  
group by merged_id, title_english, abstract_english
