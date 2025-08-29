DECLARE var_region_regex STRING DEFAULT r"^op-bach-([a-z]{2})-";
DECLARE var_status_date DATE DEFAULT DATE("{{ params.status_date }}");
WITH

urls AS (
    SELECT Region, BaseUrl
      FROM UNNEST(ARRAY<STRUCT<Region STRING, BaseUrl STRING>>[
              ("eu", "https://a45fb872cd374d85b15cb08befc40e02-dot-europe-west1.composer.googleusercontent.com/")
            , ("us", "https://68ed64xa1cd2a4a4f92b599cbe676620c-dot-us-central1.composer.googleusercontent.com/")
            , ("ca", "https://11f1c30e50c3407aa11789c6a773ec70-dot-northamerica-northeast1.composer.googleusercontent.com/")
            , ("as", "https://4afe800b744144ee9bbc0e6442f9775f-dot-asia-southeast1.composer.googleusercontent.com/")
            , ("au", "https://f10cabf79dbb43e18c7e367ec6c0ef89-dot-australia-southeast1.composer.googleusercontent.com/")
            , ("br", "https://b5db4c421b164f0897951da9eaf02e91-dot-southamerica-east1.composer.googleusercontent.com/")
      ])
)
    SELECT DagId, Description, LastRunTime, Owner
         , Environment
         , REGEXP_EXTRACT(Environment, var_region_regex) AS Region
         , FORMAT("%sdags/%s", BaseUrl, DagId) AS Url
         , Schedule, Status
         , ErrorRateLast7Days
         , ErrorRateLast30Days
         , ErrorRateLast60Days
         , FunctionalAreaCode, DataSteward
      FROM `geotab-metadata-prod.MetadataInventory_EU.AirflowPipelines`
 LEFT JOIN urls
        ON REGEXP_EXTRACT(Environment, var_region_regex) = Region
     WHERE StatusDate = var_status_date
       AND Environment LIKE "%-production"
