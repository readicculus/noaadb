from flask_marshmallow import fields
from marshmallow.fields import List
from marshmallow_sqlalchemy import auto_field

from noaadb.api.config import db, ma
from noaadb.schema.models import NOAAImage, Job, Worker, Species, Label, Hotspot, FalsePositives
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
        model = Label
        sqla_session = db.session
        include_fk = True

label_schema = LabelSchema()
labels_schema = LabelSchema(many=True)


class HotspotSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Hotspot
        sqla_session = db.session
    eo_label = Nested(LabelSchema())
    ir_label = Nested(LabelSchema())

hotspot_schema = HotspotSchema()
hotspots_schema = HotspotSchema(many=True)


class FalsePositivesSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = FalsePositives
        sqla_session = db.session

falsepositive_schema = FalsePositivesSchema()
falsepositives_schema = FalsePositivesSchema(many=True)


