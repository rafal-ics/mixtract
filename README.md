# Mixtract

Mixtract (**Mix**panel + ex**tract**) extracts [Mixpanel](https://mixpanel.com/) data, by date, via the Mixpanel API and exports to either a CSV file or an external database. Currently, CSV, MySQL, PostgreSQL, Oracle SQL, and MS SQL are supported.

## Requirements

- Python 3.5 or later.
- `arrow`
- `pandas`
- `requests`
- `sqlalchemy`
- `pyyaml`

## Directions

Install the package via pip:

```
pip install mixtract
```

Before you run the program, create `mixtract.yaml` in the directory where you wish to collect the extracted data files. The configuration YAML file should look like this:

```
secret_keys:
  - project_name_1: secret_key_string
  - project_name_2: secret_key_string
  - project_name_3: secret_key_string
mysql:
  - db_user = 'coco'
  - db_pass = 'black'
  - db_host = '192.0.0.115'
  - db_port = '3307'
  - db_name = 'squirrelfran'
postgresql:
  - db_user = '' # leaving username blank will result in connecting via UNIX socket
  - db_pass = ''
  - db_host = ''
  - db_port = ''
  - db_name = 'sample_database'
```

The valid headings are `secret_keys` (always required), `csv` (no configuration required), `mysql`, `postgresql`, `oracle`, and `mssql`.

Run

```
mixtract target start end
```

in the terminal to run `mixtract`, where:
- `target` is one of `csv`, `mysql`, `postgresql`, `oracle`, or `mssql`;
- `start` is the start date for the range of data extraction.
- `end` is the end date for the range of data extraction.
Optionally, you can supply the `--serialize` tag to obtain the `pickle` serialization file for the Pandas dataframes containing the Mixpanel data.
