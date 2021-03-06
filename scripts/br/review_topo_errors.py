# THIS is not included in the automatic processing - we run it in a notebook
# to analyze the errors and manually change the shapefiles.
import re
import pandas as pd

intersections = []
ring_self_intersections = []
purged_geometries = []
affected_file = None
with open('log/log_topology.log', 'r') as log_file:
    for line in log_file.readlines():
        if 'Generating topology from file' in line:
            match = re.match(r'file (.*)', line)
            affected_file = match.group(1)
        if 'found non-noded intersection between' in line:
            match = re.match(r'LINESTRING \((-?[0-9]\d*\.(\d+)?) (-?[0-9]\d*\.(\d+)?), (-?[0-9]\d*\.(\d+)?) (-?[0-9]\d*\.(\d+)?)\) at (-?[0-9]\d*\.(\d+)?) (-?[0-9]\d*\.(\d+)?)', line)
            intersections.append({
                'affected_file': affected_file,
                'line_a_point_a_long': match.group(1),
                'line_a_point_a_lat': match.group(3),
                'line_a_point_b_long': match.group(5),
                'line_a_point_b_lat': match.group(7),
                'line_b_point_a_long': match.group(9),
                'line_b_point_a_lat': match.group(11),
                'line_b_point_b_long': match.group(13),
                'line_b_point_b_lat': match.group(15),
                'intersect_long': match.group(17),
                'intersect_lat': match.group(19)
            })
        if 'Self-intersection at or near point' in line:
            match = re.match(r'Self-intersection at or near point (-?[0-9]\d*\.(\d+)?) (-?[0-9]\d*\.(\d+)?)', line)
            ring_self_intersections.append({
                'affected_file': affected_file,
                'long': match.group(1),
                'lat': match.group(3),
            })
        if 'invalid geometric objects' in line:
            match = re.match(r'removed (\d+) invalid geometric objects', line)
            purged_geometries.append(
                'affected_file': affected_file,
                'n_purged': match.group(1)
            )

df_intersections = pd.DataFrame(intersections)
df_ring_self_intersections = pd.DataFrame(ring_self_intersections)
df_purged_geometries = pd.DataFrame(purged_geometries)
