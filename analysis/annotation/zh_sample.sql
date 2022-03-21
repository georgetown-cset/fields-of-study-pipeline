with sample as (
  -- Take e.g. records where the (unsigned) remainder of the hash / 100 is less than 10
  -- to get a ~10% sample because ~10% of the hashes should be divisible by 100
  select merged_id
  from field_model_replication.sampling_frame
  where mod(abs(id_hash), 10000) < 1
),
meta as (
 select
   merged_id,
   id as orig_id,
   (title is not null
   and title_cld2_lid_success is true
   and title_cld2_lid_is_reliable is true
   and lower(title_cld2_lid_first_result_short_code) = 'zh') as has_title,
   title,
   (abstract is not null
   and abstract_cld2_lid_success is true
   and abstract_cld2_lid_is_reliable is true
   and lower(abstract_cld2_lid_first_result_short_code) = 'zh') as has_abstract,
   abstract,
 from gcp_cset_links_v2.all_metadata_with_cld2_lid
   -- Get merged_id
 inner join gcp_cset_links_v2.article_links on article_links.orig_id = all_metadata_with_cld2_lid.id
 inner join sample using(merged_id)
 ),
meta_ranks as (
    select
      *,
      -- Identify the longest titles and abstracts for publications. This will be non-deterministic in the case of ties,
      -- but this probably doesn't happen often unless the texts are the same or have length zero, and there's no
      -- obviously good way to break ties.
      -- At this point we're getting the longest title and abstract in other languages too, but below we pick the longest
      -- text that's in our desired language.
      row_number() over (partition by merged_id, has_title order by char_length(trim(title)) desc) as title_length_rank,
      row_number() over (partition by merged_id, has_abstract order by char_length(trim(abstract)) desc) as abstract_length_rank,
    from meta
    ),
best_text as (
   -- From the 1+ orig_id records for each merged_id, get the longest @lang title and longest @lang abstract
   select
     merged_id,
     string_agg(if(title_length_rank = 1 and has_title, title, null) limit 1) as title,
     string_agg(if(abstract_length_rank = 1 and has_abstract, abstract, null) limit 1) as abstract
   from meta_ranks
   group by merged_id
)
select
  merged_id,
  title,
  abstract,
from best_text

