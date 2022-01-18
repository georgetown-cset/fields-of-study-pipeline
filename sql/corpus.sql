-- Adapted from https://github.com/georgetown-cset/fields-of-study/blob/main/sql/en_corpus.sql
-- Parameterized with @lang (e.g. 'en' or 'zh)

with meta as (
  select
    merged_id,
    id as orig_id,
    (title is not null
      and title_cld2_lid_success is true
      and title_cld2_lid_is_reliable is true
      and lower(title_cld2_lid_first_result_short_code) = @lang) as has_title,
    title,
    (abstract is not null
      and abstract_cld2_lid_success is true
      and abstract_cld2_lid_is_reliable is true
      and lower(abstract_cld2_lid_first_result_short_code) = @lang) as has_abstract,
    abstract,
  from gcp_cset_links_v2.all_metadata_with_cld2_lid
  -- Get merged_id
  inner join gcp_cset_links_v2.article_links on article_links.orig_id = all_metadata_with_cld2_lid.id
  where
  -- This just shrinks the results a bit (to publications with @lang titles or @lang abstracts or both)
  (
    (title is not null
      and title_cld2_lid_success is true
      and title_cld2_lid_is_reliable is true
      and lower(title_cld2_lid_first_result_short_code) = @lang) is true
    or (abstract is not null
      and abstract_cld2_lid_success is true
      and abstract_cld2_lid_is_reliable is true
      and lower(abstract_cld2_lid_first_result_short_code) = @lang) is true
  )
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
),
clean_text as (
  select
    merged_id,
    trim(lower(regexp_replace(
      regexp_replace(
        case
        -- Shouldn't be possible for both title and abstract to be null;
        -- that's the implicit 'else' here, in which case we'll get null text
          when title is null and abstract is not null then abstract
          when title is not null and abstract is null then title
          when title is not null and abstract is not null then title || '. ' || abstract
        end,
        -- Language-dependent pattern for what characters we'll remove from the text:
        --   For English, remove everything but alpha, spaces, and digits; also remove lone numbers
        --   For Chinese, just remove punctuation
        case when @lang = 'zh' then '[[:punct:]]' else '([^[:alpha:]\\s\\d])|(\\b\\d+\\b)' end, ''
      ),
      -- Replace various other whitespace with spaces
      '[\\r\\n\\v\\t]+', ' '))) as text
  from best_text
)
select *
from clean_text
where 
  text is not null
  and char_length(text) > 0

