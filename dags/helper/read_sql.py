def read_sql_file(file_path):
    """
    Reads SQL syntax from a file and returns it as a string.

    Args:
        file_path (str): The path to the SQL file.

    Returns:
        str: The SQL syntax as a string, or None if an error occurs.
    """
    BASE_PATH = "/opt/airflow/dags"
    try:
        with open(f'{BASE_PATH}/{file_path}', 'r') as file:
            sql_string = file.read()
            return sql_string
    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

