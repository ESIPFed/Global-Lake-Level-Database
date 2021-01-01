from db_create import create_tables
import fill_reference_table as reference_tbls
from metadata import reference_table_metadata_json
from lake_table_grealm import update_grealm_lake_levels
from lake_table_hydroweb import update_hydroweb_lake_levels
from lake_table_usgs import update_usgs_lake_levels
from utiils import get_lake_table


def main():
    create_tables()
    while True:
        df = reference_tbls.update_reference_id_table()
        reference_table_metadata_json(df)
        existing_table = get_lake_table()
        update_grealm_lake_levels(existing_table)
        update_hydroweb_lake_levels(existing_table)
        update_usgs_lake_levels(existing_table)
if __name__ == '__main__':
    main()
