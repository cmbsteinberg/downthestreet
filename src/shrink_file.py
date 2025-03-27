import duckdb

def convert_csv_to_parquet(
    input_path: str,
    output_path: str,
    columns: list[str] | list[int],
    memory_limit: str = "4GB",
    csv_params: dict = None
) -> None:
    """
    Convert a large CSV file (compressed or uncompressed) to Parquet format, selecting specific columns.
    
    Args:
        input_path: Path to input CSV file (can be .csv or .csv.gz)
        output_path: Path for output Parquet file
        columns: List of column names or indices to select
        memory_limit: DuckDB memory limit (default "4GB")
        csv_params: Optional dictionary of CSV parameters (delimiter, header, quote char, etc.)
    """
    
    # Create connection
    con = duckdb.connect()
    
    # Set memory limit
    con.execute(f"SET memory_limit='{memory_limit}'")
    
    # Build column selection string
    # If columns are integers, convert to column0, column1 format
    col_select = ", ".join(
        f"column{c}" if isinstance(c, int) else f'"{c}"'
        for c in columns
    )
    
    # Build CSV reading parameters if provided
    csv_params_str = ""
    if csv_params:
        csv_params_str = ", ".join(
            f"{k} = '{v}'" for k, v in csv_params.items()
        )
        read_func = f"read_csv('{input_path}', {csv_params_str})"
    else:
        read_func = f"read_csv_auto('{input_path}')"
    
    # Construct and execute query
    query = f"""
        COPY (
            SELECT {col_select}
            FROM {read_func}
        )
        TO '{output_path}' (FORMAT PARQUET)
    """
    
    try:
        con.execute(query)
        print(f"Successfully converted {input_path} to {output_path}")
    except Exception as e:
        print(f"Error converting file: {str(e)}")
    finally:
        con.close()

# Example usage:
if __name__ == "__main__":
    # Using column indices
    convert_csv_to_parquet(
        input_path="../data/urls-meta-23-11-02.csv.gz",
        output_path="../data/urls-meta-23-11-02.parquet",
        columns=[0, 2, 3]
    )
