# ponder-notebooks

Tutorial and Example Notebooks for the Ponder Product


### Developer's Guide

Once you populate your credentials in `snowflake/credential.py` or `bigquery/credential.json` run this to ensure that you don't accidentally commit your credentials to git: 
```
git update-index --assume-unchanged snowflake/credential.py 
git update-index --assume-unchanged bigquery/credential.json 
```

### Note

Any commit made to the main branch will trigger an auto-commit on the [pandas-web-hub](https://github.com/ponder-org/pandas-web-hub)
