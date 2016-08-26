import sys

sys.path.append('../')
import gcmt_utils.sql_utils as sq
import json

def make_gcmt_geojson_from_table(gj_file_name, database, table):

    gj_dict = sq.make_new_geojson_from_table(table, database)

    with open(gj_file_name, 'w') as f:
        json.dump(gj_dict, f)


if __name__ == '__main__':
    make_gcmt_geojson_from_table(*sys.argv[1:])
