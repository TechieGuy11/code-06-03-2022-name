import itertools
import json
import os
from configparser import ConfigParser

import ijson
import pandas as pd

bmi_category_vs_risk = {"Underweight": "Malnutrition risk",
                        "Normal weight": "Low risk",
                        "Overweight": "Enhanced risk",
                        "Moderately obese": "Medium risk",
                        "Severely obese": "High risk",
                        "Very severely obese": "Very high risk"}


def calculate_bmi_category(bmi):
    if bmi <= 18.4:
        category = "Underweight"
    elif bmi < 25:
        category = "Normal weight"
    elif bmi < 30:
        category = "Overweight"
    elif bmi < 35:
        category = "Moderately obese"
    elif bmi <= 40:
        category = "Severely obese"
    else:
        category = "Very severely obese"
    return category


def get_batch_df(sequence, batch_size):
    df_batch = pd.DataFrame()  # initialize with empty dataframe
    batch = list(itertools.islice(sequence, batch_size))
    if batch:  # batch is not None
        df_batch = pd.DataFrame(batch)
    return df_batch


def perform_operation(df, output_json_file="output.json"):
    df['BMI'] = df.apply(lambda r: round(r.WeightKg / ((r.HeightCm / 100.00) ** 2), 4), axis=1)
    df['BMI_Category'] = df["BMI"].apply(calculate_bmi_category)
    df['Health_Risk'] = df["BMI_Category"].map(bmi_category_vs_risk)
    j = df.to_json(orient='records')
    json_list = json.loads(j)
    with open(output_json_file, 'a') as f:
        for json_data in json_list:
            json_str = str(json_data)
            json_str = json_str.replace('\'', '\"')
            f.write(json_str)
            f.write(',\n')
    return df


if __name__ == '__main__':
    # read configuration settings from config file
    config = ConfigParser()
    config.read("config.ini")
    batch_size = config.getint("CONFIGURATION", "batch_size")
    input_json_file = config.get("CONFIGURATION", "input_json_file")
    output_json_file = config.get("CONFIGURATION", "output_json_file")

    # if output file exits, remove it and start create new to start '['
    if os.path.exists(output_json_file):
        os.remove(output_json_file)
    with open(output_json_file, 'w') as f:
        f.write('[')

    with open(input_json_file, mode='r') as fp:

        sequence = ijson.items(fp, "item")  # gives python objects from json streams

        df = get_batch_df(sequence, batch_size)  # get the dataframe from the first batch

        while not df.empty:
            perform_operation(df, output_json_file)  # if batch is not empty perform operation
            df = get_batch_df(sequence, batch_size)  # get next batch dataframe
        with open(output_json_file, 'a') as f:
            f.write(']')  # end the json file with "]"
    print(f"written output file '{output_json_file}'")
