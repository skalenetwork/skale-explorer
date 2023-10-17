import os
import json
import sys
import re
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
chunk_size = 10000

db_params = {
    "host": "localhost",
    "user": "postgres",
    "database": "blockscout"
}

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


def split_string(s):
    parts = re.split(r'(\d+)', s)
    parts = [int(part) if part.isdigit() else part for part in parts]
    return parts


def get_latest_id(cursor, table_name):
    cursor.execute(f"SELECT MAX(id) FROM {table_name};")
    latest_id = cursor.fetchone()[0]
    if latest_id is None:
        latest_id = 0
    return latest_id


def get_number_of_rows(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    return row_count


def paginate_query(cursor, sql_query, table_name):
    current_page = 0
    number_of_rows = get_number_of_rows(cursor, table_name)
    while True:
        paginated_query = sql_query + f"""
            LIMIT {chunk_size}
            OFFSET {current_page * chunk_size} + (
                SELECT COUNT(*) FROM {table_name}
            ) - {number_of_rows};
        """
        cursor.execute(paginated_query)
        rows = cursor.fetchall()
        if not rows:
            break
        yield rows
        current_page += 1
        completeness = round(chunk_size * current_page * 100 / number_of_rows, 1)
        completeness = completeness if completeness < 100 else 100
        print(f"Percentage complete: {completeness}" + " " * 5, end='\r')
        time.sleep(5)
    print()


def migration_status_decorator(func):
    def wrapper(schain_name, table_name, *args, **kwargs):
        migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
        with open(migration_status_file) as f:
            migration_status = json.load(f)
        if migration_status[table_name]:
            print(f"Table {table_name}:\nSkipping, already restored")
            return

        result = func(schain_name, table_name, *args, **kwargs)

        migration_status[table_name] = True
        with open(migration_status_file, 'w') as f:
            f.write(json.dumps(migration_status, indent=4))
        return result
    return wrapper


def dump(schain_name: str, tables_metadata: list):
    schain_dump_dir = os.path.join(DUMPS_DIR_PATH, schain_name)
    os.makedirs(schain_dump_dir, exist_ok=True)
    db_params["port"] = get_db_port(schain_name)
    try:
        connection = psycopg2.connect(**db_params)
    except psycopg2.OperationalError:
        db_params["database"] = "explorer"
        connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for table_info in tables_metadata:
        table_name = table_info["table_name"]
        sql_query = table_info["sql_query"]
        table_dir = os.path.join(schain_dump_dir, table_name)
        if os.path.exists(table_dir):
            shutil.rmtree(table_dir)
        os.makedirs(table_dir)
        number_of_rows = get_number_of_rows(cursor, table_name)
        print(f"Table: {table_name}")
        print(f"Number of rows: {number_of_rows}")
        for i, rows in enumerate(paginate_query(cursor, sql_query, table_name)):
            columns = [desc[0] for desc in cursor.description]
            data = []
            for row in rows:
                formatted_data = []
                for value in row:
                    if isinstance(value, memoryview):
                        formatted_data.append(value.hex())
                    elif isinstance(value, datetime.datetime):
                        formatted_data.append(value.strftime(datetime_format))
                    elif isinstance(value, Decimal):
                        formatted_data.append(float(value))
                    elif is_list_of_decimals(value):
                        formatted_data.append([float(item) for item in value])
                    else:
                        formatted_data.append(value)
                data.append(dict(zip(columns, formatted_data)))

            if (table_name == "smart_contracts"):
                for item in data:
                    if not all(
                        key in item
                        for key in additional_columns_for_smart_contracts.keys()
                    ):
                        item.update(additional_columns_for_smart_contracts)

            table_file = os.path.join(table_dir, f"{table_name}_{i+1}.json")
            with open(table_file, "w") as f:
                json.dump(data, f, indent=4)

    default_migration_status = {q["table_name"]: False for q in tables_metadata}
    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file, "w") as f:
        json.dump(default_migration_status, f, indent=4)

    cursor.close()
    connection.close()


@migration_status_decorator
def restore_table(schain_name, table_name, transform_func=None):
    print(f"Table {table_name}:")
    db_params["port"] = get_db_port(schain_name)
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()
    cursor.execute("BEGIN")

    table_dir_path = os.path.join(DUMPS_DIR_PATH, schain_name, table_name)
    table_files = sorted(os.listdir(table_dir_path), key=split_string)
    for index, table_file in enumerate(table_files):
        table_file_dir = os.path.join(table_dir_path, table_file)
        with open(table_file_dir, "r") as f:
            table_data = json.load(f)

        if not table_data:
            print("Skipping, table is empty")
            cursor.close()
            connection.close()
            return

        if transform_func:
            table_data = transform_func(table_data, cursor)
            if not table_data:
                print("Skipping, nothing to insert")
                cursor.close()
                connection.close()
                return

        column_names = table_data[0].keys()
        insert_query = f"""
            INSERT INTO {table_name}
            ({', '.join([
                f'"{column_name}"' for column_name in column_names
            ])})
            VALUES ({', '.join(['%s'] * len(table_data[0]))})
            {on_conflict_sql(table_name, column_names)}
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
        completeness = round((index + 1) * 100 / len(table_files), 1)
        print(f"Percentage complete: {completeness}" + " " * 5, end='\r')
        time.sleep(5)

    print()
    connection.commit()
    cursor.close()
    connection.close()


def on_conflict_sql(table_name, column_names):
    if table_name == "addresses" or table_name == "blocks" or table_name == "smart_contracts" \
            or table_name == "tokens" or table_name == "token_transfers" \
            or table_name == "transactions" or table_name == "smart_contracts_additional_sources":
        return f"""ON CONFLICT ON CONSTRAINT {table_name}_pkey DO UPDATE
            SET {', '.join([f"{column_name} = EXCLUDED.{column_name}"
            for column_name in column_names])};"""
    return ""


def update_sequences(schain_name):
    db_params["port"] = get_db_port(schain_name)
    connection = psycopg2.connect(**db_params)
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute("SELECT SETVAL('address_names_id_seq', (SELECT MAX(id) FROM address_names));")
    cursor.execute(
        "SELECT SETVAL('smart_contracts_id_seq', "
        "(SELECT MAX(id) FROM smart_contracts));"
    )
    cursor.execute(
        "SELECT SETVAL('address_current_token_balances_id_seq', "
        "(SELECT MAX(id) FROM address_current_token_balances));"
    )
    cursor.execute(
        "SELECT SETVAL('smart_contracts_additional_sources_id_seq', "
        "(SELECT MAX(id) FROM smart_contracts_additional_sources));"
    )
    cursor.close()
    connection.close()


def transform_addresses(table_data, cursor=None):
    for item in table_data:
        item["hash"] = bytes.fromhex(item["hash"])
        if (item["contract_code"] is not None):
            item["contract_code"] = bytes.fromhex(item["contract_code"])
        if (item["fetched_coin_balance"] is not None):
            item["fetched_coin_balance"] = Decimal(item["fetched_coin_balance"])
    return table_data


def transform_smart_contracts(table_data, cursor=None):
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
        if item["implementation_address_hash"] is not None:
            item["implementation_address_hash"] = bytes.fromhex(item["implementation_address_hash"])
        item["abi"] = json.dumps(item["abi"])
    return table_data


def transform_address_names(table_data, cursor):
    latest_id = get_latest_id(cursor, "address_names")
    transformed_data = []
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
        cursor.execute(
            "SELECT * FROM address_names WHERE address_hash = %s LIMIT 1",
            (item["address_hash"],)
        )
        row = cursor.fetchone()
        if row is None:
            latest_id += 1
            item["id"] = latest_id
            transformed_data.append(item)
    return transformed_data


def transform_smart_contracts_additional_sources(table_data, cursor=None):
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
    return table_data


def transform_blocks(table_data, cursor=None):
    for item in table_data:
        item["difficulty"] = Decimal(item["difficulty"])
        item["gas_limit"] = Decimal(item["gas_limit"])
        item["gas_used"] = Decimal(item["gas_used"])
        item["total_difficulty"] = Decimal(item["total_difficulty"])
        item["hash"] = bytes.fromhex(item["hash"])
        item["miner_hash"] = bytes.fromhex(item["miner_hash"])
        item["nonce"] = bytes.fromhex(item["nonce"])
        item["parent_hash"] = bytes.fromhex(item["parent_hash"])
    return table_data


def transform_transactions(table_data, cursor=None):
    for item in table_data:
        item["cumulative_gas_used"] = Decimal(item["cumulative_gas_used"])
        item["gas"] = Decimal(item["gas"])
        item["gas_price"] = Decimal(item["gas_price"])
        item["gas_used"] = Decimal(item["gas_used"])
        item["hash"] = bytes.fromhex(item["hash"])
        item["input"] = bytes.fromhex(item["input"])
        item["r"] = Decimal(item["r"])
        item["s"] = Decimal(item["s"])
        item["v"] = Decimal(item["v"])
        item["value"] = Decimal(item["value"])
        item["block_hash"] = bytes.fromhex(item["block_hash"])
        item["from_address_hash"] = bytes.fromhex(item["from_address_hash"])
        if item["to_address_hash"] is not None:
            item["to_address_hash"] = bytes.fromhex(item["to_address_hash"])
        if item["created_contract_address_hash"] is not None:
            item["created_contract_address_hash"] = bytes.fromhex(
                item["created_contract_address_hash"]
            )
        if item["old_block_hash"] is not None:
            item["old_block_hash"] = bytes.fromhex(item["old_block_hash"])
    return table_data


def transform_tokens(table_data, cursor=None):
    for item in table_data:
        del item["bridged"]
        item["contract_address_hash"] = bytes.fromhex(item["contract_address_hash"])
        if item["total_supply"] is not None:
            item["total_supply"] = Decimal(item["total_supply"])
        if item["decimals"] is not None:
            item["decimals"] = Decimal(item["decimals"])
    return table_data


def transform_token_transfers(table_data, cursor=None):
    for item in table_data:
        item["transaction_hash"] = bytes.fromhex(item["transaction_hash"])
        item["from_address_hash"] = bytes.fromhex(item["from_address_hash"])
        item["to_address_hash"] = bytes.fromhex(item["to_address_hash"])
        item["token_contract_address_hash"] = bytes.fromhex(item["token_contract_address_hash"])
        item["block_hash"] = bytes.fromhex(item["block_hash"])
        if item["amount"] is not None:
            item["amount"] = Decimal(item["amount"])
        if item["token_ids"] is not None:
            item["token_ids"] = [Decimal(token_id) for token_id in item["token_ids"]]
    return table_data


def transform_address_current_token_balances(table_data, cursor):
    cursor.execute("DELETE FROM address_current_token_balances")
    for item in table_data:
        item["address_hash"] = bytes.fromhex(item["address_hash"])
        item["token_contract_address_hash"] = bytes.fromhex(item["token_contract_address_hash"])
        item["value"] = Decimal(item["value"])
        if item["old_value"] is not None:
            item["old_value"] = Decimal(item["old_value"])
        if item["token_id"] is not None:
            item["token_id"] = Decimal(item["token_id"])
    return table_data


tables_metadata = [
    {
        "table_name": "addresses",
        "sql_query": "SELECT * FROM addresses ORDER by hash",
        "transform_function": transform_addresses
    },
    {
        "table_name": "address_names",
        "sql_query": "SELECT * FROM address_names ORDER by inserted_at",
        "transform_function": transform_address_names
    },
    {
        "table_name": "smart_contracts",
        "sql_query": "SELECT * FROM smart_contracts ORDER BY id",
        "transform_function": transform_smart_contracts
    },
    {
        "table_name": "smart_contracts_additional_sources",
        "sql_query": "SELECT * FROM smart_contracts_additional_sources ORDER BY id",
        "transform_function": transform_smart_contracts_additional_sources
    },
    {
        "table_name": "blocks",
        "sql_query": "SELECT * FROM blocks ORDER BY number DESC",
        "transform_function": transform_blocks
    },
    {
        "table_name": "transactions",
        "sql_query": "SELECT * FROM transactions ORDER BY hash",
        "transform_function": transform_transactions
    },
    {
        "table_name": "tokens",
        "sql_query": "SELECT * FROM tokens ORDER by inserted_at",
        "transform_function": transform_tokens
    },
    {
        "table_name": "token_transfers",
        "sql_query": "SELECT * FROM token_transfers ORDER BY transaction_hash",
        "transform_function": transform_token_transfers
    },
    {
        "table_name": "address_current_token_balances",
        "sql_query": "SELECT * FROM address_current_token_balances ORDER BY id",
        "transform_function": transform_address_current_token_balances
    }
]


def dump_schains(only_contracts=False):
    _tables_metadata = tables_metadata[:4] if only_contracts else tables_metadata
    schain_names = get_all_names()
    for schain_name in schain_names:
        if check_db_running(schain_name):
            print("-" * 50)
            print(f"Dumping schain: {schain_name}")
            dump(schain_name, _tables_metadata)


def restore_schains(only_contracts=False):
    _tables_metadata = tables_metadata[:4] if only_contracts else tables_metadata
    schain_names = get_all_names()
    for schain_name in schain_names:
        if check_db_running(schain_name):
            print("-" * 50)
            print(f"Restoring schain {schain_name}")
            for data in _tables_metadata:
                restore_table(schain_name, data["table_name"], data["transform_function"])
            update_sequences(schain_name)


def main():
    if sys.argv[1] == "dump":
        if sys.argv[2] == "contracts":
            dump_schains(only_contracts=True)
        else:
            dump_schains()
    elif sys.argv[1] == "restore":
        if sys.argv[2] == "contracts":
            restore_schains(only_contracts=True)
        else:
            restore_schains()
    else:
        print("Specify migration mode (dump or restore)")


if __name__ == '__main__':
    main()
