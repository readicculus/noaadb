DELETE FROM noaa_test.hotspots hs
USING noaa_test.hotspots_joined hsj
WHERE hsj.hotspot_row_id=hs.id
AND hsj.species = 'Polar Bear'
RETURNING hs.*;

DELETE FROM noaa_test.labels l
USING noaa_test.labels_joined lj
WHERE lj.label_id=l.id
AND lj.species = 'Polar Bear'
RETURNING lj.*;