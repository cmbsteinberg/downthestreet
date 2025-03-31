import duckdb
import csv

ABOUT_PATTERNS = [
    # English patterns
    "/about",
    "/about-me",
    "/bio",
    "/biography",
    "/me",
    "/who-am-i",
    "/about/me",
    "/hello",
    "/introduction",
    "/personal",
    "/my-story",
    "/my-journey",
    "/about-the-author",
    "/meet-me",
    # Spanish patterns
    "/sobre-mi",
    "/quien-soy",
    "/acerca-de-mi",
    "/mi-biografia",
    "/mi-historia",
    "/biografia",
    "/presentacion",
    "/perfil",
    "/conoceme",
    "/hola",
    # German patterns
    "/ueber-mich",
    "/über-mich",
    "/ich",
    "/meine-geschichte",
    "/biografie",
    "/das-bin-ich",
    "/steckbrief",
    "/vorstellung",
    "/hallo",
    "/personliches",
    "/persönliches",
    # French patterns
    "/a-propos",
    "/qui-suis-je",
    "/biographie",
    "/ma-bio",
    "/mon-parcours",
    "/me-connaitre",
    "/me-connaître",
    "/presentation",
    "/présentation",
    "/bonjour",
    "/mon-histoire",
]


def find_about_pages_duckdb_fast(
    feeds_file,
    urls_meta_file,
    output_parquet_file=None,
):
    """
    Finds potential "about" pages in urls_meta for domains in feeds.csv using DuckDB (optimized for speed).
    Optionally writes the result to a Parquet file directly using DuckDB.

    Args:
        feeds_file (str): Path to the feeds.csv file.
        urls_meta_file (str): Path to the urls-meta.csv.gz file.
        output_parquet_file (str, optional): Path to save the output as a Parquet file using DuckDB. Defaults to None.

    Returns:
        list or None: A list of URLs that are likely "about" pages if output_parquet_file is None, otherwise None.
    """
    con = None
    try:
        # Create an in-memory DuckDB connection
        con = duckdb.connect(database=":memory:", read_only=False)

        con.execute("SET enable_progress_bar = true;")
        con.execute("SET enable_progress_bar_print = true;")
        print(con.execute("SELECT current_setting('enable_progress_bar');").fetchall())
        print(
            con.execute(
                "SELECT current_setting('enable_progress_bar_print');"
            ).fetchall()
        )

        # Load domains from feeds.csv and extract the domain
        con.execute(
            f"""
            CREATE TEMPORARY TABLE feeds AS
            SELECT
              REGEXP_EXTRACT(column2, '^(?:https?:\/\/)?([^/]+)') AS domain
            FROM read_csv('{feeds_file}', header=false)
        """
        )

        # Join ABOUT_PATTERNS for the regex pattern
        pattern_regex = "|".join(
            pattern.replace("/", "\\/") for pattern in ABOUT_PATTERNS
        )

        # Faster query for urls-meta.csv.gz and create a temporary table
        fast_query = f"""
            CREATE TEMPORARY TABLE potential_about_pages AS
            WITH url_domains AS (
                SELECT
                    column0 AS url,
                    REGEXP_EXTRACT(column0, '^(?:https?:\/\/)?([^/]+)') AS extracted_domain
                FROM read_csv('{urls_meta_file}', header=false, compression='gzip')
                WHERE REGEXP_MATCHES(column0, '({pattern_regex})($|\\?)')
            )
            SELECT DISTINCT ud.url
            FROM url_domains ud
            INNER JOIN feeds f ON ud.extracted_domain = f.domain;
        """

        # Execute the query
        con.execute(fast_query)

        # Write out the db as a parquet file if output_parquet_file is provided
        if output_parquet_file:
            con.execute(
                f"COPY potential_about_pages TO '{output_parquet_file}' (FORMAT 'PARQUET');"
            )
            print(
                f"Successfully wrote the potential about pages to {output_parquet_file}"
            )
            return None
        else:
            # Fetch the results if not writing to Parquet
            result = con.execute("SELECT url FROM potential_about_pages").fetchall()
            about_pages = [row[0] for row in result]
            return about_pages

    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        if con:
            con.close()


if __name__ == "__main__":
    feeds_file = "../data/feeds.csv"  # Assuming feeds.csv is in the data directory
    urls_meta_file = "../data/urls-meta-23-11-02.csv.gz"  # Assuming this file is also in the data directory
    output_parquet_file = (
        "../data/about_pages.parquet"  # Specify the output Parquet file path
    )

    # Call the function to write to Parquet
    find_about_pages_duckdb_fast(
        feeds_file, urls_meta_file, output_parquet_file=output_parquet_file
    )
