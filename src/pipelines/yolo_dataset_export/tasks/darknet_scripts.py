import os

import luigi

from core import ForcibleTask


class MakeDarknetScriptsTask(ForcibleTask):
    darknet_path = luigi.Parameter()
    def _generate_darknet_scripts(self):
        darknet_bin = "%s" % os.path.join(str(self.darknet_path), 'darknet')
        data_path = self.output()['yolo_data_file'].path
        args = ["detector","train", data_path, "$*"]
        bash_script_str = "#!/bin/bash\n" + \
                      ' '. join([darknet_bin]+args)+'\n'
        with self.output()['train_script'].open('w') as f:
            f.write(bash_script_str)


        # gen_anchors
        # darknet_bin = "%s" % os.path.join(str(self.darknet_path), 'darknet')
        # data_path = self.output()['yolo_data_file'].path
        # args = ["detector","calc_anchors", data_path, "-num_of_clusters", "$1", "-width", "$2", "-height", "$3"]
        # bash_script_str = "#!/bin/bash\n" + \
        #               ' '. join([darknet_bin]+args)+'\n'
        # with self.output()['train_script'].open('w') as f:
        #     f.write(bash_script_str)
