WITH answer AS (SELECT *
      FROM answer
      WHERE 1=1

          AND ((
              project_id
           IN (9080,9081,9082)))


        AND ((((
          (submission_date AT TIME ZONE 'UTC') - (interval '3.5 hour')
        ) >= ((SELECT TIMESTAMP {{_date}})) AND (
          (submission_date AT TIME ZONE 'UTC') - (interval '3.5 hour')
        ) < ((SELECT (TIMESTAMP {{_date}} + (1 || ' day')::INTERVAL))))))
       )
SELECT
    outputs."project_id"  AS "outputs.project_id",
        (DATE(((outputs."timestamp" AT TIME ZONE 'UTC') - (interval '3.5 hour')) )) AS "outputs.dc_reporting_submission_date_date",
    outputs."task_string_id"  AS "outputs.task_id",
    (answer_to_shape."shape_name")  AS "outputs.shape",
        CASE WHEN COUNT(DISTINCT CASE WHEN (task."round") = outputs."round"  THEN ( outputs."id"  )  ELSE NULL END) = 0 THEN NULL ELSE COUNT(DISTINCT CASE WHEN (task."round") = outputs."round"  THEN ( outputs."id"  )  ELSE NULL END) END AS "outputs.last_submission_count",
        CASE WHEN COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  ((task."round") = outputs."round" )  THEN  (( answer_to_shape."count"  ) )  ELSE NULL END
,0)*(1000000*1.0)) AS DECIMAL(65,0))) + ('x' || MD5(CASE WHEN  ((task."round") = outputs."round" )  THEN  (( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  ) )  ELSE NULL END
::varchar))::bit(64)::bigint::DECIMAL(65,0)  *18446744073709551616 + ('x' || SUBSTR(MD5(CASE WHEN  ((task."round") = outputs."round" )  THEN  (( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  ) )  ELSE NULL END
::varchar),17))::bit(64)::bigint::DECIMAL(65,0) ) - SUM(DISTINCT ('x' || MD5(CASE WHEN  ((task."round") = outputs."round" )  THEN  (( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  ) )  ELSE NULL END
::varchar))::bit(64)::bigint::DECIMAL(65,0)  *18446744073709551616 + ('x' || SUBSTR(MD5(CASE WHEN  ((task."round") = outputs."round" )  THEN  (( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  ) )  ELSE NULL END
::varchar),17))::bit(64)::bigint::DECIMAL(65,0)) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) = 0 THEN NULL ELSE COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  (task."round") = outputs."round"   THEN  ( answer_to_shape."count"  )   ELSE NULL END
,0)*(1000000*1.0)) AS DECIMAL(65,0))) + ('x' || MD5(CASE WHEN  (task."round") = outputs."round"   THEN  ( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  )   ELSE NULL END
::varchar))::bit(64)::bigint::DECIMAL(65,0)  *18446744073709551616 + ('x' || SUBSTR(MD5(CASE WHEN  (task."round") = outputs."round"   THEN  ( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  )   ELSE NULL END
::varchar),17))::bit(64)::bigint::DECIMAL(65,0) ) - SUM(DISTINCT ('x' || MD5(CASE WHEN  (task."round") = outputs."round"   THEN  ( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  )   ELSE NULL END
::varchar))::bit(64)::bigint::DECIMAL(65,0)  *18446744073709551616 + ('x' || SUBSTR(MD5(CASE WHEN  (task."round") = outputs."round"   THEN  ( CONCAT(outputs.task_id, (outputs."id"), (answer."answer_id"), (answer_to_shape."shape_name"))  )   ELSE NULL END
::varchar),17))::bit(64)::bigint::DECIMAL(65,0)) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) END AS "outputs.last_submission_shape_count"
FROM public.submission  AS outputs
INNER JOIN public.step  AS step ON (outputs."step_id") = (step."id")
      -- Step Table Project ID Filter

        AND ((
          (step."project_id")
         IN (9080,9081,9082)))

INNER JOIN public.task  AS task ON outputs.task_id = (task."id")

      -- TODO: We need to support null values in order to make
      -- this dimension to work. Reference: DD-782

      --
      --  AND ((((
      --    ((DATE(task."last_update_timestamp" )) AT TIME ZONE 'UTC') - (interval '3.5 hour')
      --  ) >= ((SELECT TIMESTAMP {{_date}})) AND (
      --    ((DATE(task."last_update_timestamp" )) AT TIME ZONE 'UTC') - (interval '3.5 hour')
      --  ) < ((SELECT (TIMESTAMP {{_date}} + (1 || ' day')::INTERVAL))))))
      --


        AND ((
          (task."project_id")
         IN (9080,9081,9082)))

LEFT JOIN answer ON (outputs."id") = (answer."submission_id") AND
      (outputs."timestamp") = (answer."submission_date")
      -- Answer Table Project ID Filter

        AND ((
          (answer."project_id")
         IN (9080,9081,9082)))

LEFT JOIN public.answer_to_shape  AS answer_to_shape ON (answer."answer_id") = (answer_to_shape."answer_id") AND
      (outputs."timestamp") = (answer_to_shape."submission_date")
      -- Answer To Shape Table Date Filter

        AND ((((
          ((answer_to_shape."submission_date") AT TIME ZONE 'UTC') - (interval '3.5 hour')
        ) >= ((SELECT TIMESTAMP {{_date}})) AND (
          ((answer_to_shape."submission_date") AT TIME ZONE 'UTC') - (interval '3.5 hour')
        ) < ((SELECT (TIMESTAMP {{_date}} + (1 || ' day')::INTERVAL))))))

WHERE ((step."ordinal") ) = 1 AND ((task."round") = outputs."round" ) AND ((task."state") ) IN ('acknowledged', 'approved', 'completed', 'delivered', 'in progress') AND (-- Date Filter by User

      ((((
        ((outputs."timestamp") AT TIME ZONE 'UTC') - (interval '3.5 hour')
      ) >= ((SELECT TIMESTAMP {{_date}})) AND (
        ((outputs."timestamp") AT TIME ZONE 'UTC') - (interval '3.5 hour')
      ) < ((SELECT (TIMESTAMP {{_date}} + (1 || ' day')::INTERVAL))))))



        AND ((
          (outputs."project_id")
         IN (9080,9081,9082)))
     )
GROUP BY
    1,
    2,
    3,
    4
ORDER BY
    5 DESC
--FETCH NEXT 5000 ROWS ONLY