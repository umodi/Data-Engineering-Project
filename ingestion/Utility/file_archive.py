def archive_file(source_file_path: str, archive_folder_path: str) -> bool:
    try:
        file_name = source_file_path.split("/")[-1]
        archive_file_path = f"{archive_folder_path.rstrip('/')}/{file_name}"

        dbutils.fs.mv(source_file_path, archive_file_path)

        print(f"File archived successfully: {archive_file_path}")
        return True

    except Exception as e:
        print(f"Archive failed: {str(e)}")
        raise
