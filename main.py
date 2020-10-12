from db_create import create_tables
import fill_reference_table as reference_tbls
from metadata import reference_table_mtadta_json
from lake_table_grealm import update_grealm_lake_levels
from lake_table_hydroweb import update_hydroweb_lake_levels

def main():
    create_tables()
    while True:
        user_input = str(input("Please enter thy bidding: ").lower())
        if user_input == 'replace':
            reference_tbls.replace_reference_id_table()
            print('Reference table deleted and replaced\nProcess Completed')
        elif user_input == 'update':
            update_input = str(input('Update [l]ake levels or [m]etadata?: ')).lower()
            if update_input == 'l':
                update_grealm_lake_levels()
                print('Grealm lake water levels updated')
                update_hydroweb_lake_levels()
                print('Hydroweb lake levels updated')
                print('Process Completed')
            elif update_input == 'm':
                reference_tbls.update_reference_id_table()
                print('Added new lakes')
                reference_table_mtadta_json()
                print('Updated the metadata')
                print('Process Completed')
            else:
                continue
        if user_input == 'q':
            break


if __name__ == '__main__':
    main()
