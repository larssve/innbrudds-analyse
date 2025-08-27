SELECT MONTHNAME(innbruddsdato) AS month,
       COUNT(1) AS antall
FROM innbruddstips
GROUP BY month,MONTH(innbruddsdato)
ORDER BY MONTH(innbruddsdato);
