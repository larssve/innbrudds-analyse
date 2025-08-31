SELECT MONTHNAME(innbruddsdato) AS måned,
       COUNT(1) AS antall
FROM innbruddstips
GROUP BY måned,MONTH(innbruddsdato)
ORDER BY MONTH(innbruddsdato);
