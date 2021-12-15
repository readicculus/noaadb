import luigi
from sqlalchemy import exists
from sqlalchemy.orm import joinedload
import numpy as np

from noaadb import Session
from noaadb.schema.models import *

class UpdatePartitionsTask(luigi.Task):
    def run(self):
        s = Session()
        # query EO Annotations that arent in a partition
        eo_annots_no_partition = s.query(Annotation)\
            .filter(~ exists().where(Partitions.eo_event_key==Annotation.eo_event_key))\
            .options(joinedload(Annotation.species)).all()

        eo_ims = {}
        species_counts_by_image = {}
        eo_ims_to_ir_ims = {}
        for annot in eo_annots_no_partition:
            if annot.eo_event_key is None:
                continue
            if annot.eo_event_key not in eo_ims:
                eo_ims[annot.eo_event_key] = []
                species_counts_by_image[annot.eo_event_key] = {}

            if annot.eo_event_key not in eo_ims_to_ir_ims:
                eo_ims_to_ir_ims[annot.eo_event_key] = annot.ir_event_key

            eo_ims[annot.eo_event_key].append(annot)
            if not annot.species.name in species_counts_by_image[annot.eo_event_key]:
                species_counts_by_image[annot.eo_event_key][annot.species.name] = 0
            species_counts_by_image[annot.eo_event_key][annot.species.name] += 1

        n_partitions = s.query(Partitions.partition).distinct().all()
        s.close()

        species_id = {}
        for im_key in eo_ims:
            for annot in eo_ims[im_key]:
                if annot.species.name not in species_id:
                    species_id[annot.species.name] = len(species_id)

        counts_by_partition = np.zeros(len(n_partitions))
        partitions = {k:[] for (k,) in n_partitions}
        for im_key in eo_ims:
            partition_for_image = np.argmin(counts_by_partition)
            counts_by_partition[partition_for_image] += len(eo_ims[im_key])
            partitions[int(partition_for_image)].append(im_key)

        s=Session()
        plist = []
        for partition_id in partitions:
            for eo_im_key in partitions[partition_id]:
                p=Partitions(eo_event_key=eo_im_key, ir_event_key=eo_ims_to_ir_ims[eo_im_key], partition=partition_id)
                s.add(p)
                plist.append(p)
        s.commit()
        s.close()
        print('Added %d partitions' % len(plist))
