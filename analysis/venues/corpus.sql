with corpus as (
  select
   scholar_cat,
   mag_name as journal_name,
   PaperId as paper_id,
   OriginalTitle as title,
   abstract,
   Year as year,
  from `gcp-cset-projects.field_model_replication.top_mag_venues`
  inner join `gcp-cset-projects.gcp_cset_mag.Journals`
   on DisplayName = mag_name
  inner join `gcp-cset-projects.gcp_cset_mag.PapersWithAbstracts`
   using (JournalId)
  where scholar_cat = 'eng_artificialintelligence'
)
select
  paper_id,
  scholar_cat,
  journal_name,
  year,
  trim(lower(regexp_replace(
    regexp_replace(
      case
        -- Shouldn't be possible for both title and abstract to be null;
        -- that's the implicit 'else' here, in which case we'll get null text
        when title is null and abstract is not null then abstract
        when title is not null and abstract is null then title
        when title is not null and abstract is not null then title || '. ' || abstract
      end,
      --  For English, remove everything but alpha, spaces, and digits; also remove lone numbers
      '([^[:alpha:]\\s\\d])|(\\b\\d+\\b)' , ''
      ),
  -- Replace various other whitespace with spaces
  '[\\r\\n\\v\\t]+', ' '))) as text
  from corpus