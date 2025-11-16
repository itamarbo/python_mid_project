import kagglehub
from kagglehub import KaggleDatasetAdapter
import my_pass as cred
import psycopg2
import pandas as pd
import ast
from sqlalchemy import create_engine


def get_data(file_path):
    df = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        "rounakbanik/the-movies-dataset",
        file_path,
    )
    return df


movies_metadata = get_data("movies_metadata.csv")


def extract_json_list(collection_str, field):
    if pd.notna(collection_str):
        try:
            collection_list = ast.literal_eval(collection_str)
            if isinstance(collection_list, list):
                # Extract the field from each dictionary in the list
                return [
                    item.get(field)
                    for item in collection_list
                    if isinstance(item, dict)
                ]
            return None
        except:
            return None
    return None


# Extract genre names as a list
movies_metadata["genre_names"] = movies_metadata["genres"].apply(
    lambda x: extract_json_list(x, "name")
)

cols = [
    "id",
    "title",
    "genres",
    "imdb_id",
    "popularity",
    "release_date",
    "budget",
    "revenue",
    "runtime",
    "spoken_languages",
    "vote_average",
    "vote_count",
]
movies_metadata = movies_metadata[cols]


# Cleaning data
movies_metadata["genre_names"] = movies_metadata["genres"].apply(
    lambda x: extract_json_list(x, "name")
)
movies_metadata.drop(columns=["genres"], inplace=True)

movies_metadata["languages"] = movies_metadata["spoken_languages"].apply(
    lambda x: extract_json_list(x, "name")
)
movies_metadata.drop(columns=["spoken_languages"], inplace=True)

movies_metadata["languages"] = (
    movies_metadata["languages"]
    .astype(str)
    .str.replace("[", "")
    .str.replace("]", "")
    .str.replace("'", "")
)
movies_metadata["genre_names"] = (
    movies_metadata["genre_names"]
    .astype(str)
    .str.replace("[", "")
    .str.replace("]", "")
    .str.replace("'", "")
)
movies_metadata["imdb_id"] = (
    movies_metadata["imdb_id"].astype(str).str.replace("tt", "")
)

# Data type conversion
movies_metadata["id"] = pd.to_numeric(movies_metadata["id"], errors="coerce").astype(
    "Int64"
)
movies_metadata["budget"] = pd.to_numeric(
    movies_metadata["budget"], errors="coerce"
).astype("Int64")
movies_metadata["popularity"] = pd.to_numeric(
    movies_metadata["popularity"], errors="coerce"
).astype("Float64")
# Replace non-numeric values with '0', then convert to integer
movies_metadata["imdb_id"] = (
    pd.to_numeric(movies_metadata["imdb_id"], errors="coerce").fillna(0).astype("int64")
)
movies_metadata["revenue"] = (
    pd.to_numeric(movies_metadata["revenue"], errors="coerce").fillna(0).astype("int64")
)

movies_metadata.info()

credits = get_data("credits.csv")
keywords = get_data("keywords.csv")
credits_keywords = pd.merge(credits, keywords, on="id", how="inner")
credits_keywords = credits_keywords[["id", "crew", "cast", "keywords"]]
credits_keywords
movies = pd.merge(movies_metadata, credits_keywords, on="id", how="inner")
movies["mrr_created_date"] = pd.Timestamp.now()


# Using psycopg2 with context manager
HOST = cred.v_host
DB = cred.v_database
USER = cred.v_user
PASS = cred.v_password

with psycopg2.connect(host=HOST, database=DB, user=USER, password=PASS) as conn:
    with conn.cursor() as cur:
        # Your database operations here
        # For to_sql, you still need SQLAlchemy engine
        engine = create_engine(f"postgresql://{USER}:{PASS}@{HOST}/{DB}")
        movies.to_sql("movies", engine, if_exists="replace", index=False)
        print("Data successfully loaded to 'movies' table")
