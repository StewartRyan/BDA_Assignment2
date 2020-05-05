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

import calendar
import datetime


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


def get_day_of_week(date):
    # 1. We create the output variable
    res = calendar.day_name[(datetime.datetime.strptime(date, "%d-%m-%Y")).weekday()]

    # 2. We return res
    return res


def get_hour(time):
    # 1. We create the output variable
    res = time.split(":")[0]

    # 2. We return res
    return res


def generate_weekday(entry):
    # split date and time
    date_time = entry[4].split(' ')

    # String representations of date and time
    date = date_time[0]
    time = date_time[1]

    # Extracts weekday and hour from strings
    week_day = get_day_of_week(date)
    hour = get_hour(time)

    # Setup our key eg. Friday_22
    key = week_day + "_" + hour

    return key, 1


def calc_percentages(entry, total):
    percentage = (entry[1] / total) * 100
    return entry[0], (entry[1], percentage)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name):
    # dataset -----------------> inputRDD
    inputRDD = sc.textFile(my_dataset_dir)

    # Process the raw lines into usable tuples
    mapped_lines = inputRDD.map(lambda line: process_line(line))

    # Filter above tuples by run-outs for given station only
    runout_filter = mapped_lines.filter(lambda tup: tup[0] == '0' and tup[5] == '0' and tup[1] == station_name)

    # Total run-outs for given station
    total_runouts = runout_filter.count()

    mapped_weekdays = runout_filter.map(lambda tup: generate_weekday(tup))

    # Reduce all run-out stations to get totals
    reduced_weekdays = mapped_weekdays.reduceByKey(lambda x, y: x + y)

    # Sort the reduced tuples by total run-outs in descending order
    sorted_tuples = reduced_weekdays.sortBy(lambda x: x[1], ascending=False)

    # Finalise the tuples by mapping them into the required format with the percentages
    finalised_tuples = sorted_tuples.map(lambda tup: calc_percentages(tup, total_runouts))

    # Collect the totals
    result = finalised_tuples.collect()

    # # Print the results
    for item in result:
        print(item)

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
    # 1. We use as many input arguments as needed
    station_name = "Fitzgerald's Park"

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
    my_main(sc, my_dataset_dir, station_name)
