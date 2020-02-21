drop view if exists noaa_test.hotspots_joined;
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
     l_rgb.job_name,
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
     l_rgb.flight
  FROM
     noaa_test.hotspots as hs
  LEFT JOIN noaa_test.labels_joined as l_rgb ON hs.eo_label = l_rgb.id
  LEFT JOIN noaa_test.labels_joined as l_ir ON hs.ir_label = l_ir.id
);
