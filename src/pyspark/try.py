def convert_to_mb(data_size, size_unit):
    """
    Convert the given data size to MB.

    Parameters:
    data_size (float): The size of the dataset.
    size_unit (str): The unit of the dataset size ('KB', 'MB', 'GB', 'TB').

    Returns:
    float: The size of the dataset in MB.
    """
    size_unit = size_unit.upper()
    if size_unit == 'KB':
        return data_size / 1024
    elif size_unit == 'MB':
        return data_size
    elif size_unit == 'GB':
        return data_size * 1024
    elif size_unit == 'TB':
        return data_size * 1024 * 1024
    else:
        raise ValueError("Invalid size unit. Please choose from 'KB', 'MB', 'GB', 'TB'.")


def main():
    try:
        data_size = float(input("Please enter the data size: "))
        size_unit = input("Please select your size format (KB, MB, GB, TB): ")
        data_size_mb = convert_to_mb(data_size, size_unit)
        print(f"The data size in MB is: {data_size_mb:.2f} MB")
    except ValueError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()