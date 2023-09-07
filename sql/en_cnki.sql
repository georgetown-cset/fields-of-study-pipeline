-- Select EN text in CNKI for field scoring
with cnki as (
  select
    document_name,
    publication_title_en as title,
    abstract_en as abstract,
  from gcp_cset_cnki.cnki_journals
  where
    publication_title_en is not null
    or abstract_en is not null
),
clean_text as (
  select
    document_name,
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
        '([^[:alpha:]\\s\\d])|(\\b\\d+\\b)', ''
      ),
      -- Replace various other whitespace with spaces
      '[\\r\\n\\v\\t]+', ' '))) as text
  from cnki
)
select *
from clean_text
where
  text is not null
  and char_length(text) > 0