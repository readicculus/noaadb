drop view if exists noaa_test.labels_joined;
create or replace view noaa_test.labels_joined as (
  SELECT
   l.id as label_id,
   l.hotspot_id,
   i.file_name,
   i.type as image_type,
   i.foggy as is_foggy,
   i.width as image_width,
   i.height as image_height,
   i.depth as image_depth,
   i.flight,
   i.survey,
   i.timestamp as image_timestamp,
   i.cam_position as camera_position,
   l.x1,
   l.x2,
   l.y1,
   l.y2,
   l.age_class,
   l.confidence,
   l.is_shadow,
   l.start_date,
   l.end_date,
   s.name as species,
   j.job_name as job_name,
   w.name as worker,
   i.id as img_row_id
  FROM
     noaa_test.labels as l
  INNER JOIN noaa_test.noaa_images as i ON l.image = i.id
  INNER JOIN noaa_test.species as s ON l.species = s.id
  INNER JOIN noaa_test.jobs as j ON l.job = j.id
  INNER JOIN noaa_test.workers as w ON l.worker = w.id
);