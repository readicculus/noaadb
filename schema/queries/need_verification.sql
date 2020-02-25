-- SELECT * from noaa_test.labels_joined
-- WHERE species = 'Polar Bear' and is_shadow;
-- SELECT * from noaa_test.hotspots_joined
-- WHERE rgb_x1 <=0 or rgb_y1 <= 0
SELECT * from noaa_test.labels_joined 
WHERE NOT is_shadow 
AND image_type = 'RGB'
AND
(
	(worker = 'noaa' AND (species IN ('Polar Bear', 'Ringed Seal', 'Bearded Seal', 'UNK Seal')))   -- did not get checked by me
	OR end_date IS NOT NULL -- deleted
	OR x1 < 0 OR y1 < 0 or x2 > image_width or y2 > image_height   -- out of bounds
	OR confidence < 50
)

