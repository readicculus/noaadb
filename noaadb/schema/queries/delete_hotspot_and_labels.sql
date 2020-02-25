DELETE FROM noaa_test.hotspots hs
USING noaa_test.labels l
WHERE l.id=hs.eo_label_id
AND l.species_id = 1
RETURNING hs.*;

DELETE FROM noaa_test.labels l
USING noaa_test.species s
WHERE s.id=l.species_id
AND s.name = 'Polar Bear'
RETURNING l.*;