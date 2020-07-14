from flask_marshmallow import fields
from marshmallow.fields import List
from marshmallow_sqlalchemy import auto_field

from noaadb.api.config import db, ma
from noaadb.schema.models import NOAAImage, Job, Worker, Species, EOIRLabelPair, LabelChips, Chip, \
    LabelChipBase, FPChips, LabelEntry
from marshmallow_sqlalchemy.fields import Nested

class NOAAImageSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = NOAAImage
        sqla_session = db.session

image_schema = NOAAImageSchema(exclude=['meta'])
images_schema = NOAAImageSchema(many=True,exclude=['meta'])


class JobSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Job
        sqla_session = db.session

job_schema = JobSchema()
jobs_schema = JobSchema(many=True)


class WorkerSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Worker
        sqla_session = db.session

worker_schema = WorkerSchema()
workers_schema = WorkerSchema(many=True)


class SpeciesSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Species
        sqla_session = db.session

specie_schema = SpeciesSchema()
species_schema = SpeciesSchema(many=True)


class LabelSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = LabelEntry
        sqla_session = db.session
        include_fk = True

label_schema = LabelSchema()
labels_schema = LabelSchema(many=True)


class HotspotSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = EOIRLabelPair
        sqla_session = db.session
    eo_label = Nested(LabelSchema())
    ir_label = Nested(LabelSchema())

hotspot_schema = HotspotSchema()
hotspots_schema = HotspotSchema(many=True)


class FalsePositivesSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = LabelEntry
        sqla_session = db.session

falsepositive_schema = FalsePositivesSchema()
falsepositives_schema = FalsePositivesSchema(many=True)


class ChipSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Chip
        sqla_session = db.session

chip_schema = ChipSchema()
chips_schema = ChipSchema(many=True)

class LabelChipSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = LabelChips
        sqla_session = db.session
        fields = ("percent_intersection","relative_x1","relative_x2","relative_y1","relative_y2","chip_id")

chiphotspot_schema = LabelChipSchema()
chiphotspots_schema = LabelChipSchema(many=True)

class FPChipSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = FPChips
        sqla_session = db.session
        fields = ("percent_intersection","relative_x1","relative_x2","relative_y1","relative_y2","chip_id")
fpchip_schema = FPChipSchema()
fpchips_schema = FPChipSchema(many=True)