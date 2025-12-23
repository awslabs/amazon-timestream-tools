import argparse
import pandas


def csv_to_parquet(csv_file_path: str, parquet_file_path: str) -> None:
    df: pandas.DataFrame = pandas.read_csv(csv_file_path)
    df.to_parquet(parquet_file_path, engine="pyarrow", index=False)
    print(f"Successfully converted {csv_file_path} to {parquet_file_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("csv_to_parquet")
    parser.add_argument(
        "--csv-file",
        required=True,
        help="The path to the CSV file to transform to Parquet. The new Parquet file will be created in the same directory.",
    )
    args = parser.parse_args()
    csv_file_path = args.csv_file
    csv_file_name = csv_file_path[0 : csv_file_path.rfind(".")]

    csv_to_parquet(
        csv_file_path=csv_file_path, parquet_file_path=f"{csv_file_name}.parquet"
    )
