import os
import json
import psycopg2
import datetime
from psycopg2.extras import Json
from decimal import Decimal

psycopg2.extensions.register_adapter(dict, Json)
datetime_format = '%Y-%m-%d %H:%M:%S.%f'

from_db_port = os.environ["FROM_DB_PORT"]
to_db_port = os.environ["TO_DB_PORT"]

db_params = {
    "host": "localhost",
    "user": "postgres"
}

tables = [
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


def dump():
    db_params["port"] = from_db_port
    db_params["database"] = "explorer"
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for table_info in tables:
        table_name = table_info["table_name"]
        sql_query = table_info["sql_query"]
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        print(f"Querying table: {table_name}")
        print(f"Port: {db_params['port']}")
        print(f"Received records: {len(rows)}\n")
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
        os.makedirs(f"data/{from_db_port}", exist_ok=True)
        json_filename = f"data/{from_db_port}/{table_name}.json"
        with open(json_filename, "w") as json_file:
            json.dump(data, json_file, indent=4)

    cursor.close()
    connection.close()


def get_latest_id(cursor, table_name):
    cursor.execute(f"SELECT MAX(id) FROM {table_name};")
    latest_id = cursor.fetchone()[0]
    if latest_id is None:
        latest_id = 0
    return latest_id


def migrate_addresses():
    table_name = "addresses"
    json_filename = f"data/{from_db_port}/{table_name}.json"
    with open(json_filename, "r") as json_file:
        addresses_data = json.load(json_file)

    db_params["port"] = to_db_port
    db_params["database"] = "blockscout"
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()

    for data in addresses_data:
        data_copy = data.copy()
        data["hash"] = bytes.fromhex(data["hash"])
        data["contract_code"] = bytes.fromhex(data["contract_code"])
        data["fetched_coin_balance"] = Decimal(data["fetched_coin_balance"])

        cursor.execute("SELECT * FROM addresses WHERE hash = %s LIMIT 1", (data["hash"],))
        row = cursor.fetchone()

        if row:
            row = list(row)
            row[0] = float(row[0])
            row[2] = row[2].hex()
            row[3] = row[3].hex()
            row[4] = row[4].strftime(datetime_format)
            row[5] = row[5].strftime(datetime_format)

            if (tuple(row) == tuple(data_copy.values())):
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


def migrate_smart_contracts():
    table_name = "smart_contracts"
    json_filename = f"data/{from_db_port}/{table_name}.json"
    with open(json_filename, "r") as json_file:
        smart_contracts_data = json.load(json_file)

    db_params["port"] = to_db_port
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


def migrate_smart_contracts_additional_sources():
    table_name = "smart_contracts_additional_sources"
    json_filename = f"data/{from_db_port}/{table_name}.json"
    with open(json_filename, "r") as json_file:
        smart_contracts_additional_sources_data = json.load(json_file)

    db_params["port"] = to_db_port
    db_params["database"] = "blockscout"
    connection = psycopg2.connect(**db_params)
    connection.autocommit = False
    cursor = connection.cursor()
    latest_id = get_latest_id(cursor, table_name)

    insert_query = f"""
        INSERT INTO smart_contracts_additional_sources
        ({', '.join(smart_contracts_additional_sources_data[0].keys())})
        VALUES ({', '.join(['%s'] * len(smart_contracts_additional_sources_data[0]))});
    """

    for data in smart_contracts_additional_sources_data:
        data["id"] += latest_id
        data["address_hash"] = bytes.fromhex(data["address_hash"])

    values_to_insert = [
        tuple(contract.values())
        for contract in smart_contracts_additional_sources_data]

    cursor.executemany(insert_query, values_to_insert)
    print(f"Inserted {len(smart_contracts_additional_sources_data)} records")

    connection.commit()
    cursor.close()
    connection.close()


def migrate_address_names():
    table_name = "address_names"
    json_filename = f"data/{from_db_port}/{table_name}.json"
    with open(json_filename, "r") as json_file:
        address_names_data = json.load(json_file)

    db_params["port"] = to_db_port
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


def main():
    dump()
    migrate_addresses()
    migrate_smart_contracts()
    migrate_smart_contracts_additional_sources()
    migrate_address_names()


if __name__ == '__main__':
    main()
