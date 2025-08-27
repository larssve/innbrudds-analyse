-- her ser vi at det er behov for å forbedre datakvaliteten da bislett
-- dukker opp både med lite og stor bokstav.
SELECT stedsnavn,
			 COUNT(1) AS antall
FROM 'innbruddstips-eksempel.csv'
GROUP BY stedsnavn
ORDER BY antall DESC
LIMIT 10;
