SELECT
  start_station_name,
  COUNT(start_station_name) AS number_of_journeys
FROM
  `{bq_public_data_set}.london_bicycles.cycle_hire`
GROUP BY
  start_station_name
ORDER BY
  COUNT(start_station_name) DESC