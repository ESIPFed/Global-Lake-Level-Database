from db_create import create_tables
import fill_reference_table as reference_tbls
from metadata import reference_table_metadata_json
from metadata import reference_table_metadata_json_replace
from lake_table_grealm import update_grealm_lake_levels
from lake_table_hydroweb import update_hydroweb_lake_levels
from lake_table_usgs import update_usgs_lake_levels
from lake_table_usgs import update_usgs_lake_names
from utiils import get_lake_table


def main():
    create_tables()
    while True:
        user_input = str(input("[u]pdate or [r]eplace data?: ").lower())
        if user_input == 'r':
            #reference_tbls.replace_reference_id_table()
            reference_table_metadata_json_replace()
            print('Reference table deleted and replaced\nProcess Completed')

        elif user_input == 'u':
            update_input = str(input('Update [l]ake levels or [m]etadata?: ')).lower()

            if update_input == 'l':
                print('Retrieving existing lake levels...')
                existing_table = get_lake_table()
                print('Begin Update process')
                try:
                    update_grealm_lake_levels(existing_table)
                except ValueError:
                    pass
                try:
                    update_hydroweb_lake_levels(existing_table)
                except ValueError:
                    pass
                try:
                    update_usgs_lake_levels(existing_table)
                except ValueError:
                    pass
                print('Process Completed')

            elif update_input == 'm':
                print('Checking for new Lakes from sources...')
                df = reference_tbls.update_reference_id_table()
                print('Added new lakes to DB')
                reference_table_metadata_json(df)
                print('Updated DB metadata')
                print('Process Completed!')

            else:
                continue
        if user_input == 'q':
            break

if __name__ == '__main__':
    df = main()
