SELECT
  DATE(committer.date) as sdate,
  count(*) as count
FROM
  `bigquery-public-data.github_repos.commits`
 WHERE DATE(committer.date)>='1995-01-01'
group by sdate order by sdate
;
