SELECT
  date,
  element,
  AVG(value/10) as avg_value,
  MAX(value/10) as max_value,
  MIN(value/10) as min_value
FROM
  `bigquery-public-data.ghcn_d.ghcnd_YYYYY`
WHERE
  date>='YYYYY-01-01' and date<='YYYYY-12-31'
  and value is not null
GROUP BY
  date,
  element
;
