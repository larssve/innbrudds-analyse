SELECT stedsnavn,
       COUNT(1) AS antall
FROM innbruddstips
GROUP BY stedsnavn
ORDER BY antall DESC
LIMIT 10;
