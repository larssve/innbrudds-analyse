CREATE OR REPLACE TABLE innbruddstips AS
SELECT mottatt_tid,
			 CASE
					WHEN innbruddsdato IS NOT NULL THEN innbruddsdato
			 		ELSE DATE(mottatt_tid)
			 END AS innbruddsdato,
			 LOWER(stedsnavn) AS stedsnavn
FROM (
SELECT DISTINCT(*)
FROM READ_CSV($csv_file,
							dateformat = '%Y-%m-%d',
							columns = {'mottatt_tid': 'TIMESTAMPTZ', 'innbruddsdato': 'DATE', 'stedsnavn': 'VARCHAR'},
							-- krav om å hoppe over feil i datoformat
							store_rejects = true
							));
