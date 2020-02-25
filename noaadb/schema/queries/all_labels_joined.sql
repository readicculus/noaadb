drop view if exists noaa_test.hotspots_joined;
drop view if exists noaa_test.labels_joined;
create or replace view noaa_test.labels_joined as (
  SELECT
   l.id as id,
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
   i.id as img_row_id,
   w.id as worker_row_id,
   j.id as job_row_id
  FROM
     noaa_test.labels as l
  INNER JOIN noaa_test.noaa_images as i ON l.image = i.id
  INNER JOIN noaa_test.species as s ON l.species = s.id
  INNER JOIN noaa_test.jobs as j ON l.job = j.id
  INNER JOIN noaa_test.workers as w ON l.worker = w.id
);
create or replace view noaa_test.hotspots_joined as (
  SELECT
     hs.hs_id,
     hs.eo_accepted,
     hs.ir_accepted,
     l_ir.x1 as ir_x,
     l_ir.y1 as ir_y,
     l_rgb.x1 as rgb_x1,
     l_rgb.y1 as rgb_y1,
     l_rgb.x2 as rgb_x2,
     l_rgb.y2 as rgb_y2,
     l_rgb.is_shadow,
     l_rgb.confidence,
     l_rgb.start_date,
     l_rgb.species,
     l_rgb.job_name as rgb_job,
     l_ir.job_name as ir_job,
     l_rgb.worker as rgb_worker,
     l_ir.worker as ir_worker,
     l_rgb.file_name as rgb_image,
     l_rgb.image_width as rgb_width,
     l_rgb.image_height as rgb_height,
     l_rgb.image_depth as rgb_depth,
     l_rgb.image_timestamp as rgb_timestamp,
     l_rgb.camera_position as rgb_camera_position,
     l_ir.file_name as ir_image,
     l_ir.image_width as ir_width,
     l_ir.image_height as ir_height,
     l_ir.image_depth as ir_depth,
     l_ir.image_timestamp as ir_timestamp,
     l_ir.camera_position as ir_camera_position,
     l_rgb.survey,
     l_rgb.flight,
     l_ir.id as ir_label_row_id,
     l_rgb.id as rgb_label_row_id,
     hs.id as hotspot_row_id,
     l_rgb.img_row_id as rgb_img_row_id,
     l_ir.img_row_id as ir_img_row_id
  FROM
     noaa_test.hotspots as hs
  LEFT JOIN noaa_test.labels_joined as l_rgb ON hs.eo_label = l_rgb.id
  LEFT JOIN noaa_test.labels_joined as l_ir ON hs.ir_label = l_ir.id
);
