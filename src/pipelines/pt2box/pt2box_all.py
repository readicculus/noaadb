import luigi

from pipelines.pt2box.tasks import AllPt2BoxTask, StatsPt2BoxTask
from pipelines.pt2box.tasks.draw_pt2box import DrawBoundingBoxesTask
from pipelines.pt2box.tasks.update_pt2box import ValidateUpdateTask, CommitUpdateTask

if __name__ == '__main__':
    # luigi.build([LoadTrainTestValidTask(), AllPt2BoxTask()], local_scheduler=True)
    # luigi.build([DrawBoundingBoxesTask()], local_scheduler=True)
    # a = AllPt2BoxTask(delta_crop=10, percentile=90, intersected_point_delta=4)
    # b = AllPt2BoxTask(delta_crop=10, percentile=90, intersected_point_delta=3)
    c = AllPt2BoxTask(delta_crop=8, percentile=85, intersected_point_delta=3)
    # d = AllPt2BoxTask(delta_crop=10, percentile=85, intersected_point_delta=3)
    # e = AllPt2BoxTask(delta_crop=10, percentile=85, intersected_point_delta=4) ## this one looks promising
    # luigi.build([c], local_scheduler=True)
    luigi.build([StatsPt2BoxTask(input_root=c.output().path)], local_scheduler=True)
    luigi.build([CommitUpdateTask(input_root=c.output().path)], local_scheduler=True)
    # luigi.build([DrawBoundingBoxesTask(run_dir="10_90_2"),DrawBoundingBoxesTask(run_dir="10_90_3"),DrawBoundingBoxesTask(run_dir="10_90_4")], local_scheduler=True)
