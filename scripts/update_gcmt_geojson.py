import sys

sys.path.append('../')
import gcmt_utils.db_utils as db
import json

def update_gcmt_geojson_from_table(gj_file_name, database, table):
    
    with open(gj_file_name, 'r') as f:
        gj_dict = json.load(f)

    gj_dict = db.update_geojson_from_table(table, database, gj_dict)

    with open(gj_file_name, 'w') as f:
        json.dump(gj_dict, f)


if __name__ == '__main__':
    update_gcmt_geojson_from_table(*sys.argv[1:])
