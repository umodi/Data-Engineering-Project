def validate_file(file_path: str) -> bool:

    try:
        file_info = dbutils.fs.ls(file_path)

        if len(file_info) != 1:
            raise Exception(f"Validation failed. Invalid file path: {file_path}")

        file = file_info[0]

        if file.size == 0:
            raise Exception(f"Validation failed. File is empty: {file_path}")

        print(f"Validation successful: {file_path}")
        return True

    except Exception as e:
        print(f"Exception: {str(e)}")
        raise
 
