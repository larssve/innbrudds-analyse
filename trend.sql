-- må gjøre group by MONTH(innbruddsdato) for å kunne sortere etter
-- måned.
SELECT MONTHNAME(innbruddsdato) AS month,
       COUNT(1) AS antall
FROM 'innbruddstips-eksempel.csv'
GROUP BY month,MONTH(innbruddsdato)
ORDER BY MONTH(innbruddsdato);
