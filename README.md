# Movies ETL Project

A Python ETL (Extract, Transform, Load) pipeline that processes movie data from Kaggle's "The Movies Dataset" and loads it into a PostgreSQL database.

## Overview

This project downloads movie metadata, credits, and keywords from Kaggle, performs data cleaning and transformation, and loads the processed data into a PostgreSQL database for analysis.

## Features

- **Data Extraction**: Automatically downloads movie datasets from Kaggle using `kagglehub`
- **Data Transformation**:
  - Parses JSON-formatted fields (genres, languages, cast, crew, keywords)
  - Cleans and standardizes data types
  - Merges multiple datasets (metadata, credits, keywords)
  - Calculates movie success status based on budget vs. revenue
- **Data Loading**: Loads transformed data into PostgreSQL using SQLAlchemy

## Dataset

Source: [The Movies Dataset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset) on Kaggle

The dataset includes:

- **movies_metadata.csv**: Movie information including budget, revenue, genres, languages, etc.
- **credits.csv**: Cast and crew information
- **keywords.csv**: Movie keywords/tags

## Project Structure

```
naya_project_python/
├── main.py              # Main ETL script
├── movies.ipynb         # Jupyter notebook for exploration and development
├── my_pass.py           # Database credentials (not tracked in git)
├── requirements.txt     # Python dependencies
├── proj_env/           # Virtual environment
└── README.md           # This file
```

## Prerequisites

- Python 3.12+
- PostgreSQL database
- Kaggle account and API credentials

## Installation

1. **Clone the repository**:

   ```bash
   cd naya_project_python
   ```

2. **Create and activate virtual environment**:

   ```bash
   python -m venv proj_env
   source proj_env/bin/activate  # On macOS/Linux
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Kaggle API**:

   - Place your `kaggle.json` credentials file in `~/.kaggle/`
   - Or set environment variables: `KAGGLE_USERNAME` and `KAGGLE_KEY`

5. **Configure database credentials**:
   Create a `my_pass.py` file with your PostgreSQL credentials:
   ```python
   v_host = "your_host"
   v_database = "your_database"
   v_user = "your_username"
   v_password = "your_password"
   ```

## Usage

### Run the ETL Pipeline

```bash
python main.py
```

The script will:

1. Download the latest movie datasets from Kaggle
2. Clean and transform the data
3. Load the processed data into the `movies` table in PostgreSQL

### Explore with Jupyter Notebook

```bash
jupyter notebook movies.ipynb
```

## Data Transformations

### Extracted Fields

From JSON strings to readable formats:

- **Genres**: List of genre names
- **Languages**: List of spoken languages
- **Cast**: Cast information from credits
- **Crew**: Crew information from credits
- **Keywords**: Movie keywords

### Calculated Fields

- **status**: "Success" if revenue > budget, else "Not success"
- **mrr_created_date**: Timestamp when record was processed

### Data Type Conversions

- `id`: Integer (Int64)
- `budget`: Integer (Int64)
- `revenue`: Integer (int64)
- `popularity`: Float (Float64)
- `imdb_id`: Integer (int64, extracted from 'tt' prefix)

## Database Schema

The `movies` table includes:

- `id`: Movie ID
- `title`: Movie title
- `imdb_id`: IMDB ID (numeric)
- `popularity`: Popularity score
- `release_date`: Release date
- `budget`: Production budget
- `revenue`: Box office revenue
- `runtime`: Movie runtime in minutes
- `vote_average`: Average user rating
- `vote_count`: Number of votes
- `genre_names`: Comma-separated genre names
- `languages`: Comma-separated language names
- `cast`: Cast information (JSON)
- `crew`: Crew information (JSON)
- `keywords`: Movie keywords (JSON)
- `status`: Success/Not success based on revenue vs budget
- `mrr_created_date`: Record creation timestamp

## Technologies Used

- **Python 3.12**
- **pandas**: Data manipulation and analysis
- **kagglehub**: Kaggle dataset integration
- **SQLAlchemy**: Database ORM and connection
- **psycopg2**: PostgreSQL adapter
- **Jupyter**: Interactive development

## Error Handling

The main script includes error handling to catch and report exceptions during execution. Common issues:

- Kaggle API authentication failures
- Database connection errors
- Data type conversion errors (handled with `errors='coerce'`)

## Notes

- The script uses `low_memory=False` when loading CSVs to avoid mixed type warnings
- JSON fields are parsed using `ast.literal_eval()` for safe evaluation
- Non-numeric values are coerced to NaN and then filled with 0 where appropriate
- The script uses `.copy()` to avoid pandas SettingWithCopyWarning

## License

This project is for educational purposes.

## Acknowledgments

- Dataset provided by [Rounak Banik](https://www.kaggle.com/rounakbanik) on Kaggle
- Original data from [The Movie Database (TMDb)](https://www.themoviedb.org/)
