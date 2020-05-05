# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark

import codecs
from datetime import datetime

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res


def generate_times_dates(entry):
    # split date and time
    date_time = entry[4].split(' ')

    # String representations of date and time
    date = datetime.strptime(date_time[0], '%d-%m-%Y').strftime('%Y-%m-%d')
    current_time = datetime.strptime(date_time[1], '%H:%M:%S').strftime('%H:%M:%S')

    hour_minutes = current_time.split(':')
    hour = int(hour_minutes[0])
    minutes = int(hour_minutes[1])

    total_minutes = (hour * 60) + minutes

    return date, (current_time, total_minutes)


def aggregate_actual_runouts(tup, measurement):
    result = {}
    start_key = last_time = None

    for entry in tup[1]:
        current_time = entry[1]

        try:
            difference_in_minutes = current_time - last_time

            if difference_in_minutes > measurement:
                start_key = entry
        except:
            start_key = entry

        result[start_key] = result.get(start_key, 0) + 1

        last_time = current_time

    return tup[0], result


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name, measurement_time):
    # dataset -----------------> inputRDD
    inputRDD = sc.textFile(my_dataset_dir)

    # Process the raw lines into usable tuples
    mapped_lines = inputRDD.map(lambda line: process_line(line))

    # Filter above tuples by run-outs for given station only
    runout_filter = mapped_lines.filter(lambda tup: tup[0] == '0' and tup[5] == '0' and tup[1] == station_name)

    # Map the dates and times
    mapped_times_dates = runout_filter.map(lambda tup: generate_times_dates(tup))

    # Group all together by date
    grouped_times_dates = mapped_times_dates.groupByKey()

    map_totals = grouped_times_dates.map(lambda tuples: aggregate_actual_runouts(tuples, measurement_time))

    # Sort the reduced tuples by date in ascending order
    sorted_tuples = map_totals.sortBy(lambda x: x[0])

    # Collect the totals
    result = sorted_tuples.collect()

    # # # Print the results
    for tup in result:
        for k, v in tup[1].items():
            print("('" + str(tup[0]) + "', ('" + str(k[0]) + "', " + str(v) + "))")


# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input parameters as needed
    station_name = "Fitzgerald's Park"
    measurement_time = 5

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset
    my_local_path = "/Users/ryan/College/Year 4/Big Data & Analytics/Assignment2/"
    my_databricks_path = "/"

    my_dataset_dir = "my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name, measurement_time)
