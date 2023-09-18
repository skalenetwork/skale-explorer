import os
import json
import sys
import psycopg2
import datetime
from psycopg2.extras import Json
from decimal import Decimal
from admin import DUMPS_DIR_PATH
from admin.core.containers import get_db_port, check_db_running
from admin.core.endpoints import get_all_names

psycopg2.extensions.register_adapter(dict, Json)
datetime_format = '%Y-%m-%d %H:%M:%S.%f'

db_params = {
    "host": "localhost",
    "user": "postgres"
}

table_queries = [
    {
        "table_name": "addresses",
        "sql_query": "SELECT * FROM addresses WHERE contract_code IS NOT NULL"
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
        "table_name": "address_names",
        "sql_query": "SELECT * FROM address_names"
    }
]

additional_columns = {
    "contract_code_md5": "",
    "implementation_name": None,
    "implementation_address_hash": None,
    "implementation_fetched_at": None,
    "compiler_settings": []
}

default_migration_status = {
    "addresses": False,
    "address_names": False,
    "smart_contracts": False,
    "smart_contracts_additional_sources": False
}


def get_latest_id(cursor, table_name):
    cursor.execute(f"SELECT MAX(id) FROM {table_name};")
    latest_id = cursor.fetchone()[0]
    if latest_id is None:
        latest_id = 0
    return latest_id


def dump(schain_name: str):
    db_params["port"] = get_db_port(schain_name)
    db_params["database"] = "explorer"
    try:
        connection = psycopg2.connect(**db_params)
    except psycopg2.OperationalError:
        db_params["database"] = "blockscout"
        connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for table_info in table_queries:
        table_name = table_info["table_name"]
        sql_query = table_info["sql_query"]
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        print(f"Table: {table_name}")
        print(f"Received records: {len(rows)}")
        data = []
        for row in rows:
            data_row = []
            for value in row:
                if isinstance(value, memoryview):
                    data_row.append(value.hex())
                elif isinstance(value, datetime.datetime):
                    data_row.append(value.strftime(datetime_format))
                elif isinstance(value, Decimal):
                    data_row.append(float(value))
                else:
                    data_row.append(value)
            data.append(dict(zip(columns, data_row)))

        if (table_name == "smart_contracts"):
            for item in data:
                item.update(additional_columns)
        if (table_name == "address_names"):
            data = [{"id": i + 1, **entry} for i, entry in enumerate(data)]
        os.makedirs(f"{DUMPS_DIR_PATH}/{schain_name}", exist_ok=True)
        table_file = f"{DUMPS_DIR_PATH}/{schain_name}/{table_name}.json"
        with open(table_file, "w") as f:
            json.dump(data, f, indent=4)

    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file, "w") as json_file:
        json.dump(default_migration_status, json_file, indent=4)

    cursor.close()
    connection.close()


def restore_addresses(schain_name: str):
    print("Table addresses:")
    table_name = "addresses"

    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file) as f:
        migration_status = json.load(f)
    if (migration_status[table_name]):
        print("Skipping, already restored")
        return

    table_file = f"{DUMPS_DIR_PATH}/{schain_name}/{table_name}.json"
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

    migration_status[table_name] = True
    with open(migration_status_file, 'w') as f:
        f.write(json.dumps(migration_status, indent=4))


def restore_address_names(schain_name: str):
    print("Table address_names:")
    table_name = "address_names"

    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file) as f:
        migration_status = json.load(f)
    if (migration_status[table_name]):
        print("Skipping, already restored")
        return

    table_file = f"{DUMPS_DIR_PATH}/{schain_name}/{table_name}.json"
    with open(table_file, "r") as f:
        address_names_data = json.load(f)

    if (not address_names_data):
        print("Skipping, table is empty")
        return

    db_params["port"] = get_db_port(schain_name)
    db_params["database"] = "blockscout"
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()
    latest_id = get_latest_id(cursor, table_name)

    insert_query = f"""
        INSERT INTO address_names
        ({', '.join([
            f'"{column_name}"' for column_name in address_names_data[0].keys()
        ])})
        VALUES ({', '.join(['%s'] * len(address_names_data[0]))});
    """

    for data in address_names_data:
        data["id"] += latest_id
        data["address_hash"] = bytes.fromhex(data["address_hash"])

    values_to_insert = [tuple(contract.values()) for contract in address_names_data]
    cursor.executemany(insert_query, values_to_insert)

    print(f"Inserted {len(address_names_data)} records")

    connection.commit()
    cursor.close()
    connection.close()

    migration_status[table_name] = True
    with open(migration_status_file, 'w') as f:
        f.write(json.dumps(migration_status, indent=4))


def restore_smart_contracts(schain_name: str):
    print("Table smart_contracts:")
    table_name = "smart_contracts"

    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file) as f:
        migration_status = json.load(f)
    if (migration_status[table_name]):
        print("Skipping, already restored")
        return

    table_file = f"{DUMPS_DIR_PATH}/{schain_name}/{table_name}.json"
    with open(table_file, "r") as f:
        smart_contracts_data = json.load(f)

    if (not smart_contracts_data):
        print("Skipping, table is empty")
        return

    db_params["port"] = get_db_port(schain_name)
    db_params["database"] = "blockscout"
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()
    latest_id = get_latest_id(cursor, table_name)

    insert_query = f"""
        INSERT INTO smart_contracts
        ({', '.join(smart_contracts_data[0].keys())})
        VALUES ({', '.join(['%s'] * len(smart_contracts_data[0]))});
    """

    for data in smart_contracts_data:
        data["id"] += latest_id
        data["abi"] = json.dumps(data["abi"])
        data["address_hash"] = bytes.fromhex(data["address_hash"])
        if data["implementation_address_hash"] is not None:
            data["implementation_address_hash"] = bytes.fromhex(data["implementation_address_hash"])

    values_to_insert = [tuple(contract.values()) for contract in smart_contracts_data]
    cursor.executemany(insert_query, values_to_insert)

    print(f"Inserted {len(smart_contracts_data)} records")

    connection.commit()
    cursor.close()
    connection.close()

    migration_status[table_name] = True
    with open(migration_status_file, 'w') as f:
        f.write(json.dumps(migration_status, indent=4))


def restore_smart_contracts_additional_sources(schain_name: str):
    print("Table smart_contracts_additional_sources:")
    table_name = "smart_contracts_additional_sources"

    migration_status_file = os.path.join(DUMPS_DIR_PATH, schain_name, "migration_status.json")
    with open(migration_status_file) as f:
        migration_status = json.load(f)
    if (migration_status[table_name]):
        print("Skipping, already restored")
        return

    table_file = f"{DUMPS_DIR_PATH}/{schain_name}/{table_name}.json"
    with open(table_file, "r") as f:
        smart_contracts_additional_sources_data = json.load(f)

    if (not smart_contracts_additional_sources_data):
        print("Skipping, table is empty")
        return

    db_params["port"] = get_db_port(schain_name)
    db_params["database"] = "blockscout"
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()

    insert_query = f"""
        INSERT INTO smart_contracts_additional_sources
        ({', '.join(smart_contracts_additional_sources_data[0].keys())})
        VALUES ({', '.join(['%s'] * len(smart_contracts_additional_sources_data[0]))});
    """

    for data in smart_contracts_additional_sources_data:
        data["address_hash"] = bytes.fromhex(data["address_hash"])

    values_to_insert = [
        tuple(contract.values())
        for contract in smart_contracts_additional_sources_data]

    cursor.executemany(insert_query, values_to_insert)
    print(f"Inserted {len(smart_contracts_additional_sources_data)} records")

    connection.commit()
    cursor.close()
    connection.close()

    migration_status[table_name] = True
    with open(migration_status_file, 'w') as f:
        f.write(json.dumps(migration_status, indent=4))


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
            restore_addresses(schain_name)
            restore_address_names(schain_name)
            restore_smart_contracts(schain_name)
            restore_smart_contracts_additional_sources(schain_name)


def main():
    if (sys.argv[1] == "dump"):
        dump_all()
    elif (sys.argv[1] == "restore"):
        restore_all()


if __name__ == '__main__':
    main()
