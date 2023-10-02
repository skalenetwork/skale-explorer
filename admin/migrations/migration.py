import os
import json
import sys
import time
import shutil
import datetime
import psycopg2
from psycopg2.extras import Json
from decimal import Decimal
from admin import DUMPS_DIR_PATH
from admin.core.containers import get_db_port, check_db_running
from admin.core.endpoints import get_all_names

psycopg2.extensions.register_adapter(dict, Json)
datetime_format = '%Y-%m-%d %H:%M:%S.%f'
chunk_size = 50000

db_params = {
    "host": "localhost",
    "user": "postgres",
    "database": "blockscout"
}

table_queries = [
    {
        "table_name": "addresses",
        "sql_query": "SELECT * FROM addresses WHERE contract_code IS NOT NULL"
    },
    {
        "table_name": "address_names",
        "sql_query": "SELECT * FROM address_names ORDER by inserted_at"
    },
    {
        "table_name": "smart_contracts",
        "sql_query": "SELECT * FROM smart_contracts ORDER BY id"
    },
    {
        "table_name": "smart_contracts_additional_sources",
        "sql_query": "SELECT * FROM smart_contracts_additional_sources ORDER BY id"
    },
    {
        "table_name": "tokens",
        "sql_query": "SELECT * FROM tokens ORDER by inserted_at"
    },
    {
        "table_name": "token_transfers",
        "sql_query": "SELECT * FROM token_transfers ORDER BY transaction_hash"
    }
]

additional_columns_for_smart_contracts = {
    "contract_code_md5": "",
    "implementation_name": None,
    "implementation_address_hash": None,
    "implementation_fetched_at": None,
    "compiler_settings": []
}


def is_list_of_decimals(value):
    if isinstance(value, list):
        return all(isinstance(item, Decimal) for item in value)
    return False


def get_latest_id(cursor, table_name):
    cursor.execute(f"SELECT MAX(id) FROM {table_name};")
    latest_id = cursor.fetchone()[0]
    if latest_id is None:
        latest_id = 0
    return latest_id

def get_number_of_rows(cursor, table_name):
    if (table_name == "addresses"):
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE contract_code IS NOT NULL")
    else:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    return row_count

def paginate_query(cursor, sql_query, number_of_rows):
    current_page = 1
    all_rows = []
    while True:
        paginated_query = sql_query + f"""
            LIMIT {chunk_size}
            OFFSET {(current_page - 1) * chunk_size};
        """
        cursor.execute(paginated_query)
        rows = cursor.fetchall()
        if not rows:
            break
        all_rows += rows
        current_page += 1
        print(f"Percentage complete: {round(len(all_rows) * 100 / number_of_rows, 1)}" + " " * 5, end='\r')
        time.sleep(5)
    print()
    return all_rows


def migration_status_decorator(func):
    def wrapper(schain_name, table_name, *args, **kwargs):
        migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
        with open(migration_status_file) as f:
            migration_status = json.load(f)
        if migration_status[table_name]:
            print(f"Table {table_name}: Skipping, already restored")
            return

        result = func(schain_name, table_name, *args, **kwargs)

        migration_status[table_name] = True
        with open(migration_status_file, 'w') as f:
            f.write(json.dumps(migration_status, indent=4))
        return result
    return wrapper


def dump_in_chunks(schain_dump_dir, table_name, data):
    table_dir = os.path.join(schain_dump_dir, table_name)
    if os.path.exists(table_dir):
        shutil.rmtree(table_dir)
    os.makedirs(table_dir)

    for i, chunk in enumerate(split_data(data, chunk_size)):
        table_file = os.path.join(table_dir, f"{table_name}_{i}.json")
        with open(table_file, "w") as f:
            json.dump(chunk, f, indent=4)

def split_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def dump(schain_name: str):
    db_params["port"] = get_db_port(schain_name)
    try:
        connection = psycopg2.connect(**db_params)
    except psycopg2.OperationalError:
        db_params["database"] = "explorer"
        connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for table_info in table_queries:
        table_name = table_info["table_name"]
        sql_query = table_info["sql_query"]
        print(f"Table: {table_name}")
        number_of_rows = get_number_of_rows(cursor, table_name)
        print(f"Number of rows: {number_of_rows}")
        rows = paginate_query(cursor, sql_query, number_of_rows)
        columns = [desc[0] for desc in cursor.description]
        data = []
        for row in rows:
            formatted_rows = []
            for value in row:
                if isinstance(value, memoryview):
                    formatted_rows.append(value.hex())
                elif isinstance(value, datetime.datetime):
                    formatted_rows.append(value.strftime(datetime_format))
                elif isinstance(value, Decimal):
                    formatted_rows.append(float(value))
                elif is_list_of_decimals(value):
                    formatted_rows.append([float(element) for element in value])
                else:
                    formatted_rows.append(value)
            data.append(dict(zip(columns, formatted_rows)))

        if (table_name == "smart_contracts"):
            for item in data:
                if not all(key in item for key in additional_columns_for_smart_contracts.keys()):
                    item.update(additional_columns_for_smart_contracts)
        schain_dump_dir = os.path.join(DUMPS_DIR_PATH, schain_name)
        os.makedirs(schain_dump_dir, exist_ok=True)
        dump_in_chunks(schain_dump_dir, table_name, data)
    default_migration_status = {q["table_name"]: False for q in table_queries}
    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file, "w") as f:
        json.dump(default_migration_status, f, indent=4)

    cursor.close()
    connection.close()

@migration_status_decorator
def restore_addresses(schain_name: str, table_name):
    print("Table addresses:")

    table_file = os.path.join(DUMPS_DIR_PATH, schain_name, table_name, f"{table_name}_0.json")
    with open(table_file, "r") as f:
        addresses_data = json.load(f)

    db_params["port"] = get_db_port(schain_name)
    db_params["database"] = "blockscout"
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()

    for data in addresses_data:
        data_copy = data.copy()
        data["hash"] = bytes.fromhex(data["hash"])
        data["contract_code"] = bytes.fromhex(data["contract_code"])
        if (data["fetched_coin_balance"] is not None):
            data["fetched_coin_balance"] = Decimal(data["fetched_coin_balance"])

        cursor.execute("SELECT * FROM addresses WHERE hash = %s LIMIT 1", (data["hash"],))
        row = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]

        if row:
            row = dict(zip(columns, row))
            if (row["fetched_coin_balance"] is not None):
                row["fetched_coin_balance"] = float(row["fetched_coin_balance"])
            if (row["contract_code"] is not None):
                row["contract_code"] = row["contract_code"].hex()
            row["hash"] = row["hash"].hex()
            row["inserted_at"] = row["inserted_at"].strftime(datetime_format)
            row["updated_at"] = row["updated_at"].strftime(datetime_format)

            if (row == data_copy):
                print(f"SKIP: {data['hash'].hex()}")
                continue

            try:
                update_query = f"""
                    UPDATE addresses
                    SET {', '.join([f"{column} = %s" for column in data.keys()])}
                    WHERE hash = %s
                """
                update_values = list(data.values()) + [data["hash"]]
                cursor.execute(update_query, update_values)
                connection.commit()
                print(f"UPDATE: {data['hash'].hex()}")
                continue
            except Exception as e:
                connection.rollback()
                print(e)

        try:
            insert_query = f"""
                INSERT INTO addresses
                ({', '.join(data.keys())})
                VALUES ({', '.join(['%s'] * len(data))});
            """
            cursor.execute(insert_query, tuple(data.values()))
            connection.commit()
            print(f"INSERT: {data['hash'].hex()}")
        except Exception as e:
            connection.rollback()
            print(e)

    cursor.close()
    connection.close()


@migration_status_decorator
def restore_table(schain_name, table_name, db_params, transform_func=None):
    db_params["port"] = get_db_port(schain_name)
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()
    cursor.execute("BEGIN")

    table_dir_path = os.path.join(DUMPS_DIR_PATH, schain_name, table_name)
    table_files = os.listdir(table_dir_path)
    for table_file in table_files:
        table_file_dir = os.path.join(table_dir_path, table_file)
        with open(table_file_dir, "r") as f:
            table_data = json.load(f)

        if not table_data:
            print(f"Table {table_name}: Skipping, table is empty")
            cursor.close()
            connection.close()
            return

        if transform_func:
            table_data = transform_func(table_data, cursor)

        insert_query = f"""
            INSERT INTO {table_name}
            ({', '.join([
                f'"{column_name}"' for column_name in table_data[0].keys()
            ])})
            VALUES ({', '.join(['%s'] * len(table_data[0]))});
        """

        values_to_insert = [tuple(row.values()) for row in table_data]
        try:
            cursor.executemany(insert_query, values_to_insert)
        except Exception as e:
            connection.rollback()
            cursor.close()
            connection.close()
            print(f"Error: {e}")
            exit()
        time.sleep(5)

    print(f"Table {table_name}: Inserted {len(table_data)} records")

    connection.commit()
    cursor.close()
    connection.close()


def transform_smart_contracts(table_data, cursor=None):
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
        if item["implementation_address_hash"] is not None:
            item["implementation_address_hash"] = bytes.fromhex(item["implementation_address_hash"])
        item["abi"] = json.dumps(item["abi"])
    return table_data

def transform_address_names(table_data, cursor):
    latest_id = get_latest_id(cursor, "address_names")
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
        cursor.execute("SELECT * FROM address_names WHERE address_hash = %s LIMIT 1", (item["address_hash"],))
        row = cursor.fetchone()
        if row is None:
            latest_id += 1
            item["id"] = latest_id
    return table_data

def transform_smart_contracts_additional_sources(table_data, cursor=None):
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
    return table_data


def dump_all():
    schain_names = get_all_names()
    for schain_name in schain_names:
        if check_db_running(schain_name):
            print("-" * 50)
            print(f"Dumping schain: {schain_name}")
            dump(schain_name)


def restore_all():
    schain_names = get_all_names()
    for schain_name in schain_names:
        if check_db_running(schain_name):
            print("-" * 50)
            print(f"Restoring schain {schain_name}")
            restore_addresses(schain_name, "addresses")
            restore_table(schain_name, "address_names", db_params, transform_func=transform_address_names)
            restore_table(schain_name, "smart_contracts", db_params, transform_smart_contracts)
            restore_table(schain_name, "smart_contracts_additional_sources", db_params, transform_smart_contracts_additional_sources)


def main():
    if (sys.argv[1] == "dump"):
        dump_all()
    elif (sys.argv[1] == "restore"):
        restore_all()
    else:
        print("Specify migration mode (dump or restore)")


if __name__ == '__main__':
    main()
