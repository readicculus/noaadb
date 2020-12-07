import unittest
from noaadb import Session
from api import get_eo_images, get_ir_images
from noaadb.schema.models import *
from sqlalchemy import desc


class SessionTestCase(unittest.TestCase):
    def test_session(self):
        s = Session()
        s.close()

num_images_per_flight = \
    {
        'fl07': {
            'C': {'eo' :8909 , 'ir': 8911},
            'L': {'eo' :8836, 'ir': 8836}
        },
        'fl06': {
            'C': {'eo' :9670, 'ir': 9671},
            'L': {'eo' :9672, 'ir': 9672}
        },
        'fl05': {
            'C': {'eo' :3183, 'ir': 3185},
            'L': {'eo' :3630, 'ir': 3630}
        },
        'fl04': {
            'C': {'eo' :5109, 'ir': 5110},
            'L': {'eo' :5117, 'ir': 5116}
        },
    }
class KotzImagesTestCase(unittest.TestCase):
    def test_number_images(self):
        s = Session()
        eo_images = get_eo_images(s, survey='test_kotz_2019')
        self.assertEqual(len(eo_images), 72542) # number of eo images
        ir_images = get_ir_images(s, survey='test_kotz_2019')
        self.assertEqual(len(ir_images), 72547) # number of ir images
        sum_eo = 0
        sum_ir = 0
        for fl in num_images_per_flight:
            for cam in num_images_per_flight[fl]:
                sum_eo += num_images_per_flight[fl][cam]['eo']
                sum_ir += num_images_per_flight[fl][cam]['ir']

        self.assertEqual(sum_ir, 54131)
        self.assertEqual(sum_eo, 54126)

        s.close()

    def test_number_images_per_flight(self):
        s = Session()
        for fl in num_images_per_flight:
            for cam in num_images_per_flight[fl]:
                eo_images = get_eo_images(s, survey='test_kotz_2019', flight=fl, cam=cam)
                self.assertEqual(len(eo_images), num_images_per_flight[fl][cam]['eo'])
                ir_images = get_ir_images(s, survey='test_kotz_2019', flight=fl, cam=cam)
                self.assertEqual(len(ir_images), num_images_per_flight[fl][cam]['ir'])
        s.close()

    # check that flight durations seem reasonable aka. that I didn't import data with a faulty timestamp parser
    def test_timestamps_by_flight(self):
        seconds_in_hour = 3600
        s = Session()
        for fl in num_images_per_flight:
            cams_in_flight_durations = []
            for cam in num_images_per_flight[fl]:
                # get start and end image and calculate duration from that
                start = s.query(EOImage).join(HeaderMeta).join(Camera) \
                    .filter(Camera.cam_name == cam).join(Flight).filter(Flight.flight_name == fl).join(Survey).filter(Survey.name == 'test_kotz_2019') \
                .order_by(EOImage.timestamp).first()

                end = s.query(EOImage).join(HeaderMeta).join(Camera)\
                    .filter(Camera.cam_name == cam).join(Flight).filter(Flight.flight_name == fl).join(Survey).filter(Survey.name == 'test_kotz_2019')\
                    .order_by(desc(EOImage.timestamp)).first()
                duration = end.timestamp-start.timestamp
                cams_in_flight_durations.append(duration)
                print('%s_%s %s' % (fl, cam, str(duration)))
                self.assertLess(duration.seconds, seconds_in_hour*4) # assert is less than 4 hours per flight

            # assert that the cameras within a flight are all within a 15 minute duration
            for a in cams_in_flight_durations:
                for b in cams_in_flight_durations:
                    diff = abs(a.seconds-b.seconds)
                    self.assertLess(diff, seconds_in_hour/4)

        s.close()


if __name__ == '__main__':
    unittest.main()
