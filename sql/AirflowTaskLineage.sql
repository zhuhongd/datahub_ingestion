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

, tasks AS (
    SELECT ExecutionTime
         , Location
         , PipelineID
         , TaskID
         , DestinationTable
         , CASE WHEN p.Region = "europe" THEN "eu"
                ELSE p.Region
            END AS Region
         , PipelineRunTime
         , FORMAT("%sdags/%s/grid?search=%s&task_id=%s", BaseUrl, PipelineId, PipelineId, TaskId) AS Url
      FROM `geotab-metadata-prod.Lineage.PipelineToTable` p
 LEFT JOIN urls
        ON REGEXP_EXTRACT(Location, var_region_regex) = urls.Region
     WHERE 1=1
       AND DATE(ExecutionTime) BETWEEN var_status_date - 7 AND var_status_date
       AND Location LIKE "op-bach-%-production"
   QUALIFY ROW_NUMBER() OVER (PARTITION BY Location, PipelineID, TaskID
                                  ORDER BY ExecutionTime DESC) = 1
)

    SELECT ExecutionTime
         , Location AS Environment
         , PipelineId AS DagId
         , TaskID AS TaskId
         , ARRAY_AGG(DISTINCT DestinationTable IGNORE NULLS) AS DownstreamTables
         , Region
         , PipelineRunTime
         , Url
      FROM tasks
  GROUP BY ExecutionTime, Environment, DagId, TaskId, Region, PipelineRunTime, Url
