import streamlit as st


class ExecutorConfigurationError(Exception):
    pass


def calculate_resources(cores_per_node, ram_per_node, num_nodes):
    """
    Calculate the total available memory and cores after reserving resources for management.

    Parameters:
    cores_per_node (int): Number of cores per node.
    ram_per_node (int): GB of RAM per node.
    num_nodes (int): Total number of nodes in the cluster.

    Returns:
    dict: A dictionary containing the total available cores and memory.
    """
    # Reserve 1 core and 1GB per node for resource management
    reserved_cores_per_node = 1
    reserved_memory_per_node = 1

    # Reserve 1GB for driver handling
    reserved_memory_driver = 1

    # Calculate available resources per node
    available_cores_per_node = cores_per_node - reserved_cores_per_node
    available_memory_per_node = ram_per_node - reserved_memory_per_node - reserved_memory_driver

    # Calculate total available resources
    total_available_cores = available_cores_per_node * num_nodes
    total_available_memory = available_memory_per_node * num_nodes

    return {
        "total_available_cores": total_available_cores,
        "total_available_memory_gb": total_available_memory,
        "available_cores_per_node": available_cores_per_node,
        "available_memory_per_node_gb": available_memory_per_node
    }


def calculate_executor_configurations(total_available_cores, total_available_memory, available_cores_per_node,
                                      available_memory_per_node):
    """
    Calculate thin, thick, and optimized executor configurations.

    Parameters:
    total_available_cores (int): Total available cores in the cluster.
    total_available_memory (int): Total available memory in the cluster (GB).
    available_cores_per_node (int): Available cores per node.
    available_memory_per_node (int): Available memory per node (GB).

    Returns:
    dict: A dictionary containing executor configurations.

    Raises:
    ExecutorConfigurationError: If the requested configuration exceeds the available cores per node.
    """

    # Thin executors: 2 cores per executor
    thin_executor_cores = 2
    if thin_executor_cores > available_cores_per_node:
        raise ExecutorConfigurationError("Thin executor configuration exceeds the available cores per node.")

    thin_executors = total_available_cores // thin_executor_cores
    thin_executor_memory = total_available_memory // thin_executors
    thin_executor_memory_per_core = thin_executor_memory / thin_executor_cores

    # Thick executors: All cores of a node per executor
    thick_executor_cores = available_cores_per_node
    thick_executors = total_available_cores // thick_executor_cores
    thick_executor_memory = available_memory_per_node
    thick_executor_memory_per_core = thick_executor_memory / thick_executor_cores

    # Optimized executors: 4 cores per executor
    optimized_executor_cores = 4
    if optimized_executor_cores > available_cores_per_node:
        raise ExecutorConfigurationError("Optimized executor configuration exceeds the available cores per node.")

    optimized_executors = total_available_cores // optimized_executor_cores
    optimized_executor_memory = total_available_memory // optimized_executors
    optimized_executor_memory_per_core = optimized_executor_memory / optimized_executor_cores

    return {
        "thin": {
            "executors": thin_executors,
            "executor_cores": thin_executor_cores,
            "executor_memory_gb": thin_executor_memory,
            "executor_memory_per_core_gb": thin_executor_memory_per_core
        },
        "thick": {
            "executors": thick_executors,
            "executor_cores": thick_executor_cores,
            "executor_memory_gb": thick_executor_memory,
            "executor_memory_per_core_gb": thick_executor_memory_per_core
        },
        "optimized": {
            "executors": optimized_executors,
            "executor_cores": optimized_executor_cores,
            "executor_memory_gb": optimized_executor_memory,
            "executor_memory_per_core_gb": optimized_executor_memory_per_core
        }
    }


def datasize_mb_convert(datasize, size_unit):
    """
    Convert the given data size to MB.

    Parameters:
    datasize (float): The size of the dataset.
    size_unit (str): The unit of the dataset size ('KB', 'MB', 'GB', 'TB').

    Returns:
    float: The size of the dataset in MB.

    Raises:
    ValueError: If the size unit is invalid.
    """
    size_unit = size_unit.upper()

    if size_unit == 'KB':
        return datasize / 1024
    elif size_unit == 'MB':
        return datasize
    elif size_unit == 'GB':
        return datasize * 1024
    elif size_unit == 'TB':
        return datasize * 1024 * 1024
    else:
        raise ValueError("Invalid size unit. Please choose from 'KB', 'MB', 'GB', 'TB'.")


def get_optimized_shuffle_partition(data_size, optimal_data_size):
    """
    Calculate the optimized number of shuffle partitions based on the dataset size.

    Parameters:
    data_size (float): The size of the dataset in MB.
    optimal_data_size (int): The optimal size of each partition in MB.

    Returns:
    int: The optimized number of shuffle partitions.
    """
    partitions = max(1, int(data_size / optimal_data_size))
    return partitions


# Streamlit app

st.title("Spark Executor and Shuffle Partition Optimizer")

# Input for cluster configuration
num_nodes = st.slider("Number of Nodes", min_value=1, max_value=10, value=3)
cores_per_node = st.selectbox("Cores per Node", options=[4, 8, 16, 32])
ram_per_node = st.selectbox("RAM per Node (GB)", options=[8, 16, 32, 64])

# Calculate resources and executor configurations
resources = calculate_resources(cores_per_node, ram_per_node, num_nodes)
try:
    executor_configs = calculate_executor_configurations(
        resources['total_available_cores'],
        resources['total_available_memory_gb'],
        resources['available_cores_per_node'],
        resources['available_memory_per_node_gb']
    )

    st.write("Total Available Cores:", resources['total_available_cores'])
    st.write("Total Available Memory (GB):", resources['total_available_memory_gb'])

    st.write("\nThin Executors Configuration:")
    st.write(f"Executors: {executor_configs['thin']['executors']}")
    st.write(f"Executor Cores: {executor_configs['thin']['executor_cores']}")
    st.write(f"Executor Memory (GB): {executor_configs['thin']['executor_memory_gb']}")
    st.write(f"Executor Memory per Core (GB): {executor_configs['thin']['executor_memory_per_core_gb']:.2f}")

    st.write("\nThick Executors Configuration:")
    st.write(f"Executors: {executor_configs['thick']['executors']}")
    st.write(f"Executor Cores: {executor_configs['thick']['executor_cores']}")
    st.write(f"Executor Memory (GB): {executor_configs['thick']['executor_memory_gb']}")
    st.write(f"Executor Memory per Core (GB): {executor_configs['thick']['executor_memory_per_core_gb']:.2f}")

    st.write("\nOptimized Executors Configuration:")
    st.write(f"Executors: {executor_configs['optimized']['executors']}")
    st.write(f"Executor Cores: {executor_configs['optimized']['executor_cores']}")
    st.write(f"Executor Memory (GB): {executor_configs['optimized']['executor_memory_gb']}")
    st.write(f"Executor Memory per Core (GB): {executor_configs['optimized']['executor_memory_per_core_gb']:.2f}")

    # Input for data size
    datasize = st.number_input("Enter the data size", min_value=0.0, value=1.0)
    size_unit = st.selectbox("Enter the size unit", options=['KB', 'MB', 'GB', 'TB'])
    data_size_mb = datasize_mb_convert(datasize, size_unit)

    # Calculate optimized shuffle partitions
    optimal_data_size = 200  # Optimal partition size in MB
    shuffle_partitions = get_optimized_shuffle_partition(data_size_mb, optimal_data_size)
    st.write(f"\nOptimized Shuffle Partitions: {shuffle_partitions}")

    # Determine the best executor configuration
    best_config = None
    for config_name, config in executor_configs.items():
        if config['executor_cores'] * 2 <= resources['available_cores_per_node']:
            best_config = config_name
            break

    if best_config:
        st.write(f"\nBest Executor Configuration: {best_config.capitalize()}")
        st.write(f"Number of Executors Needed: {executor_configs[best_config]['executors']}")
    else:
        st.write("\nNo suitable executor configuration found.")

except ExecutorConfigurationError as e:
    st.write(f"Configuration Error: {e}")