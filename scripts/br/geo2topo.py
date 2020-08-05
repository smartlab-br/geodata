import topojson
import json
import os
import sys
from numpy import linspace
import multiprocess
import logging
import logging.config
import pandas as pd


class Geo2Topo:
    def __init__(self, args):
        self.curr_script_dir = "/".join(args[0].split('/')[:-1])
        self.base_dir = "../.."
        self.skip_existing = True

        if len(args) > 1:
            self.base_dir = args[1]
        if len(args) > 2:
            self.skip_existing = args[2]
        self.total_done = 0
        self.total_files = 0

        self.quality_levels = list(linspace(0, 0.01, 4))
        self.quality_levels.reverse()

        # Guarantees the dirs and log files exist
        # log_base_dir = '/opt/pipelines/resources/geodata/logs'
        log_base_dir = '/var/log/pipelines/geodata'
        os.makedirs(os.path.dirname(f'{log_base_dir}/log_topology.log'), exist_ok=True)
        os.makedirs(os.path.dirname(f'{log_base_dir}/log_error.log'), exist_ok=True)
        # Creates the loggers
        self.topo_logger = self.setup_logger('shapely.geos', f'{log_base_dir}/log_topology.log', logging.INFO)
        self.logger = self.setup_logger('general_logger', f'{log_base_dir}/log_error.log', logging.ERROR)

    def update_progress(self, step):
        self.total_done = self.total_done + step
        print(
            f"Converting from geojson to topojson: {self.total_done}/{self.total_files} [{int(self.total_done / self.total_files * 100)}%]",
            end="\r",
            flush=True
        )

    @staticmethod
    def setup_logger(name, log_file, level=logging.INFO):
        """ To setup as many loggers as you want """
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger(name)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False

        return logger

    def convert(self, origin, dest):
        """ Skip if all levels have already been generated and the flag is set """
        if self.skip_existing and all(
                [
                    os.path.isfile(dest.replace('q0', f'q{quality_key+1}'))
                    for
                    quality_key, quality_simpl
                    in
                    enumerate(self.quality_levels)
                ]
        ):
            self.update_progress(len(self.quality_levels))
            return

        with open(origin, 'r') as json_file:
            data = json.load(json_file)
            # json_file.close() # Just to make sure it releases memory
        try:
            self.topo_logger.warning(f'Generating topology from file {origin}')
            tj_base = topojson.Topology(data.get('features'), presimplify=False, prequantize=False, topology=True)
        except Exception as _e:
            self.topo_logger.error(f">>>>> Error in file {origin} <<<<<", exc_info=True)
            return

        if '_q0' not in origin:  # Do not loop if quality is already set in geojson.
            if self.skip_existing and os.path.isfile(dest):
                self.update_progress(1)
                return
            tj = tj_base.toposimplify(self.quality_levels[-1])
            if not os.path.exists(os.path.dirname(dest)):
                os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w") as topo_file:
                json.dump(tj.to_dict(), topo_file)
            self.update_progress(1)
            return
        for quality_key, quality_simpl in enumerate(self.quality_levels):
            dest_q = dest.replace('q0', f'q{quality_key+1}')
            # Skips if destination fila already exists and the flag is set
            if self.skip_existing and os.path.isfile(dest_q):
                self.update_progress(1)
                continue
            tj = tj_base.toposimplify(quality_simpl)
            if not os.path.exists(os.path.dirname(dest_q)):
                os.makedirs(os.path.dirname(dest_q), exist_ok=True)
            with open(dest_q, "w") as topo_file:
                json.dump(tj.to_dict(), topo_file)
                # topo_file.close() # Just to make sure it releases memory
            self.update_progress(1)

    def run(self):
        print(f"Starting conversion from geojson to topojson...", end='\r', flush=True)

        meta_args = []
        for root, dirs, files in os.walk(f"{self.base_dir}/geojson"):
            path = root.replace(f"{self.base_dir}/geojson", "")
            for file in files:
                if file.endswith(".json"):
                    meta_args.append({
                        'size': os.stat(os.path.join(root, file)).st_size,
                        'arg': (
                            '{}/geojson{}/{}'.format(self.base_dir, path, file),
                            '{}/topojson{}/{}'.format(self.base_dir, path, file)
                        )
                    })
                    if '_q0' in file:
                        self.total_files = self.total_files + len(self.quality_levels)
                    else:
                        self.total_files = self.total_files + 1

        # loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        # print(loggers)

        print(f"Creating threads for topojson generation: {self.total_files}", end="\r", flush=True)
        with multiprocess.Pool(processes=4) as pool:
            pool.starmap(self.convert, list(pd.DataFrame(meta_args).sort_values(by=["size"])['arg']))
            # pool.close() # Just to make sure it releases memory

        print(f"All topojson files generated!!!!")


# Run the code
Geo2Topo(sys.argv).run()
