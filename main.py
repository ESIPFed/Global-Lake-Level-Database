from db_create import create_tables
import fill_reference_table as reference_tbls
from metadata import reference_table_mtadta_json
from lake_table_grealm import update_grealm_lake_levels
from lake_table_hydroweb import update_hydroweb_lake_levels
from lake_table_usgs import update_usgs_lake_levels

def main():
    create_tables()
    while True:
        user_input = str(input("[u]pdate or [r]eplace data?]: ").lower())
        if user_input == 'r':
            reference_tbls.replace_reference_id_table()
            print('Reference table deleted and replaced\nProcess Completed')

        elif user_input == 'u':
            update_input = str(input('Update [l]ake levels or [m]etadata?: ')).lower()

            if update_input == 'l':
                update_grealm_lake_levels()
                print('Grealm lake water levels updated')
                update_hydroweb_lake_levels()
                print('Hydroweb lake levels updated')
                update_usgs_lake_levels()
                print('USGS lake levels updated')
                print('Process Completed')

            elif update_input == 'm':
                df = reference_tbls.update_reference_id_table()
                print('Added new lakes to DB')
                reference_table_mtadta_json(df)
                print('Updated DB metadata')
                print('Process Completed')

            else:
                continue
        if user_input == 'q':
            break


if __name__ == '__main__':
    main()
