'''
Default Setting
Default Value: The default number of shuffle partitions in Spark is 200.
Factors to Consider
Data Size and Distribution:

Large Datasets: For large datasets, you might need more partitions to ensure parallelism and efficient data processing. For example, if you have 10 million records, setting a higher number of partitions (e.g., 1000) can help distribute the workload evenly2.
Small Datasets: For smaller datasets, fewer partitions might be sufficient. Too many partitions can lead to overhead without significant performance gains.
Cluster Resources:

CPU and Memory: Consider the number of CPU cores and the amount of memory available in your cluster. A good rule of thumb is to set the number of partitions to 2-3 times the number of available cores3.
Resource Utilization: Ensure that the partitions are not too small, as this can lead to underutilization of resources. Each partition should ideally be less than 200 MB4.
Query Complexity:

Complex Queries: More complex queries, such as those involving large joins or aggregations, may benefit from a higher number of partitions to distribute the workload and reduce shuffle overhead2.

'''


def main():
    print("This is a main function")

def datasize_mb_convert(datasize,size_unit):
    size_unit = size_unit.upper()
    if size_unit == 'KB':
        return datasize/1024
    elif size_unit == 'MB':
        return datasize
    elif size_unit == 'GB':
        return datasize * 1024
    elif size_unit == 'TB':
        return datasize * 1024 * 1024
    else:
        raise ValueError("Invalid size unit.Please choose from 'KB','MB','GB','TB' ")

def get_optimized_shuffule_partition(data_size,optmial_data_size):

    partitions = max(1,int(data_size/optmial_data_size))
    return partitions


if __name__ == "__main__":
    main()
    optmial_data_size = 200
    datasize = 1024
    size_unit = 'KB'
    data_size_mb = int(datasize_mb_convert(datasize,size_unit))
    # print(f"input {datasize}{size_unit} converted datasize is {data_size_mb}mb ")
    shuffel_partition = get_optimized_shuffule_partition(datasize,optmial_data_size)
    print(shuffel_partition)

