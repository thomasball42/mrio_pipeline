"""
Examines the data - "simple"
"""

import pandas as pd
import os
import readline
pd.options.display.float_format = "{:,.4g}".format
results_location = "results"


def main():
    os.system('cls' if os.name == 'nt' else 'clear')

    years = input("\nEnter years to examine (comma-separated, e.g., 2020,2021).\nYears: ")
    try:
        years_list = [int(year.strip()) for year in years.split(",")]
        for year in years_list:
            if year < 2010 or year > 2021:
                raise ValueError("Year out of range")
    except:
        print("Invalid input for years. Please enter comma-separated integers between 2010 and 2021.")
        return

    invalid_data_type = True
    data_type: int = 0
    while invalid_data_type:
        data_type = int(input("""\nEnter data type to examine.
1 = TradeMatrix
2 = TradeMatrixFeed
3 = impacts_full
4 = impacts_aggregated
5 = impacts_domestic
6 = impacts_overseas
Data Type: """))
        if data_type in [1, 2, 3, 4, 5, 6]:
            invalid_data_type = False
        else:
            print("Invalid data type. Please enter a number between 1 and 6.")
    files = []
    names = []
    if data_type == 1:
        files = [f"{results_location}/{year}/.mrio/TradeMatrix_import_dry_matter.csv" for year in years_list]
        names = [f"TradeMatrix {year}" for year in years_list]
    elif data_type == 2:
        files = [f"{results_location}/{year}/.mrio/TradeMatrixFeed_import_dry_matter.csv" for year in years_list]
        names = [f"TradeMatrixFeed {year}" for year in years_list]

    else:
        countries = []
        invalid_country = True
        while invalid_country:
            country_input = input("\nEnter countries to examine (comma-separated, e.g., USA,CHN,IND)\n Countries: ")
            countries = [country.strip().upper() for country in country_input.split(",")]
            c_true = [country in os.listdir(f"{results_location}/{years_list[0]}") for country in countries]
            if all(c_true):
                invalid_country = False
            else:
                print("One or more invalid or missing country codes. Please try again.")
        
        for country in countries:
            for year in years_list:
                if data_type == 3:
                    files.append(f"{results_location}/{year}/{country}/impacts_full.csv")
                    names.append(f"impacts_full {country} {year}")
                elif data_type == 4:
                    files.append(f"{results_location}/{year}/{country}/impacts_aggregated.csv")
                    names.append(f"impacts_aggregated {country} {year}")
                elif data_type == 5:
                    files.append(f"{results_location}/{year}/{country}/df_{country.lower()}.csv")
                    names.append(f"impacts_domestic {country} {year}")
                elif data_type == 6:
                    files.append(f"{results_location}/{year}/{country}/df_os.csv")
                    names.append(f"impacts_overseas {country} {year}")

    # Clear the console for better readability
    os.system('cls' if os.name == 'nt' else 'clear')
    print("Loading files...")


    for i, file in enumerate(files):
        if not os.path.exists(file):
            print(f"File not found: {names[i]} at {file}")
            files.pop(files.index(file))

    if len(files) == 0:
        print("No valid files to examine. Exiting.")
        return
    dataframes = []
    for file in files:
        df = pd.read_csv(file)
        if "Unnamed: 0" in df.columns and df["Unnamed: 0"].dtype == int:
            df = df.drop(columns=["Unnamed: 0"])
        dataframes.append(df)

    os.system('cls' if os.name == 'nt' else 'clear')



    columns = []
    filters = []
    sum_to_print = []
    count = 0

    while True:
        if count != 0:
            os.system('cls' if os.name == 'nt' else 'clear')
        count += 1


        for i, df in enumerate(dataframes):
            if len(columns) == 0:
                columns = df.columns


            print_df = apply_filters(df, filters)

            print(names[i], "\n", print_df[columns], end="\n\n")
        
        if len(sum_to_print) > 0:
            print("Sum Results: ")
            for s in sum_to_print:
                print(s)
            print("\n")

        print("Filters Applied: ")
        print(", ".join(filters).replace(":", " ") if len(filters) > 0 else "None", end="\n\n")


        print("""Options:
0: Columns
1: Filter
2: Sort
3: Sum
4: Reset Filters
5: Reset Files
6: Choose New Files""")
    
        option:int = -1
        invalid_option = True
        while invalid_option:
            try:
                option = int(input("Select an option (0-6): "))
            except:
                print("Invalid input. Please enter a number between 0 and 6.")
            if option in [0, 1, 2, 3, 4, 5, 6]:
                invalid_option = False


        if option == 0:
            dataframes, columns = display_columns(dataframes, names)

        elif option == 1:
            dataframes, filters = filter_dataframes(dataframes, columns, filters)

        elif option == 2:
            dataframes = sort(dataframes, columns)

        elif option == 3:
            sum_to_print = col_sum(dataframes, columns, names, sum_to_print, filters)

        elif option == 4:
            filters = []


        elif option == 5:
            dataframes = []
            columns = []
            filters = []
            for file in files:
                df = pd.read_csv(file)
                if "Unnamed: 0" in df.columns and df["Unnamed: 0"].dtype == int:
                    df = df.drop(columns=["Unnamed: 0"])
                dataframes.append(df)

        elif option == 6:
            return


def display_columns(dataframes, names):
    file_columns = []
    f = dataframes[0]
    print(f"\nColumns available:", end="\n")
    for i, col in enumerate(f.columns):
        print(f"{i}: {col}", end="\n")    
    print("\nSelect columns to display (comma-separated indices, e.g., 0,2,5).")
    columns = input("Columns: ")
    if columns.strip() == "":
        return dataframes, f.columns.tolist()
    columns = [int(column.strip()) for column in columns.split(",") if int(column.strip()) < len(f.columns) and int(column.strip()) >= 0]
    selected_columns = [f.columns[column] for column in columns]
    return dataframes, selected_columns

def sort(dataframes, columns):
    print("\nSelect column to sort by:")
    for i, col in enumerate(columns):
        print(f"{i}: {col}", end="\n")    
    sort_column = -1
    invalid_sort_column = True
    while invalid_sort_column:
        try:
            sort_column = int(input("Sort Column: "))
        except:
            print("Invalid input. Please enter a valid column index.")
        if sort_column >= 0 and sort_column < len(columns):
            invalid_sort_column = False

    sort_col_name = columns[sort_column]
    order = input("Sort order (asc/desc): ").strip().lower()
    ascending = True if order == "asc" else False

    sorted_dataframes = []
    for df in dataframes:
        sorted_df = df.sort_values(by=sort_col_name, ascending=ascending)
        sorted_dataframes.append(sorted_df)

    return sorted_dataframes


def col_sum(dataframes, columns, names, sum_to_print, filters):
    print("\nSelect column to sum:")
    columns = [c for c in columns if dataframes[0][c].dtype in [int, float] ]
    for i, col in enumerate(columns):
        print(f"{i}: {col}", end="\n")    
    sum_column = -1
    invalid_sum_column = True
    while invalid_sum_column:
        try:
            sum_column = int(input("Sum Column: "))
        except:
            print("Invalid input. Please enter a valid column index.")
        if sum_column >= 0 and sum_column < len(columns):
            invalid_sum_column = False

    sum_col_name = columns[sum_column]
    for i, df in enumerate(dataframes):
        df2 = apply_filters(df, filters)
        total = df2[sum_col_name].sum()
        if total > 100:
            total = total.round(2)
        sum_to_print.append(f"Sum of {sum_col_name} in {names[i]}: {'{:,}'.format(total)}, with filters: {", ".join(filters).replace(":", " ") if len(filters) > 0 else "None"}")
    return sum_to_print


def filter_dataframes(dataframes, columns, filters):
    print("\nSelect column to filter by:")
    for i, col in enumerate(columns):
        print(f"{i}: {col}", end="\n")    
    filter_column = -1
    invalid_filter_column = True
    while invalid_filter_column:
        try:
            filter_column = int(input("Filter Column: "))
        except:
            print("Invalid input. Please enter a valid column index.")
        if filter_column >= 0 and filter_column < len(columns):
            invalid_filter_column = False

    filter_col_name = columns[filter_column]

    filter_type = input("Filter type (=/!=/contains/>/<) (`c` is a valid option for `contains`): ").strip()
    if filter_type not in ["=", "contains", ">", "<", "c", "C", "!="]:
        print("Invalid filter type. Defaulting to '='.")
        filter_type = "="
    filter_value = input("Filter value: ").strip()
    new_filters = f"{filter_col_name}:{filter_type}:{filter_value}"
    filters.append(new_filters)

    return dataframes, filters


def apply_filters(dataframe_origin, filters):
    dataframe = dataframe_origin.copy()
    for f in filters:
        parts = f.split(":")
        col_name = parts[0]
        filter_type = parts[1]
        filter_value = parts[2]
        

        if filter_type in ["contains", "c", "C"]:
            dataframe = dataframe[dataframe[col_name].astype(str).str.contains(f"(?i){filter_value}")]
        elif filter_type == ">":
            dataframe = dataframe[pd.to_numeric(dataframe[col_name], errors='coerce') > float(filter_value)]
        elif filter_type == "<":
            dataframe = dataframe[pd.to_numeric(dataframe[col_name], errors='coerce') < float(filter_value)]
        elif filter_type == "!=":
            dataframe = dataframe[dataframe[col_name].astype(type(filter_value)) != str(filter_value)]
        else:
            dataframe = dataframe[dataframe[col_name].astype(type(filter_value)) == filter_value]

    return dataframe


    


if __name__ == "__main__":
    while True:
        main()
