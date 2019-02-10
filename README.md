# Migrate Data from Redshift to BigQuery

All the scripts were used for a migration from Redshift to BigQuery.

The scripts are not generic.
A lot of names, paths, etc. are hard-coded.

The time to run the migrations:
```
number of files = number of tables * number of days of data in tables

50 tables with 100 days data per table:
>= 5,000 files

Running them one after the other.
Assuming migrating 1 table with 100 days of data takes 10 min:
50 * 10 = 500 min <= 10h
```

## Setup

This is not tested and complete.
For a different migration project the scripts most likely need to be changed.

* install _virtualenv_ and _virtualenvwrapper_ and create a _venv_

* install Python packages
```
pip install -r requirements.txt
```

* Clone the forked and modified _BigShift_ from _github_
```
git clone https://github.com/RawIron/bigshift/tree/support-daily-partitions
```

* install the required Ruby gems using _bundle_
```
gem install bundle
bundle install
```

* add AWS access keys to your env
```
AWS_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

* install AWS command line tools
```
pip install awscli --upgrade --user
PATH="$HOME/.local/bin:$PATH"
```

* create buckets on AWS S3 and GCP gcs


## Tips

* Do not migrate columns of "type" _identity_

* Use the *--steps* option and run the steps one-by-one. Fix if anything broke and try again. Move on to the next step when ready.

* Start with one table, one day

* Next try all tables, one day

* Remove the *--partition_day* option in *migrate_partition.sh* to migrate a non-partitioned table

* Make a backup of your _csv_ files

* Control what is migrated or worked on by editing the appropriate _csv_ file


## Example Migration

The team "ef4" of the "zephyrus" company migrates 2 projects from RS to BQ

* bora
* ostro


## Quick Start: DAY-Partitioned

> *Caution* Please make sure the hard-coded names, paths etc. are correct.

* Set the *bigshift_home* variable in *migrate_partition.sh*

* Create a CSV file with: tablename,min(date(timestamp))
```
python3 db_tables.py --rs --daily --tables "your_project"
```

* Run the migration: unload
```
# bigshift with --step unload
bash iter_table_partitions.sh "your_csv_file" "end_day" >n.out 2>&1
```

* Search log file _n.out_ for Postgres-Client errors
```
cat n.out | grep -B 5 PG: | grep "bash migrate" | cut -d " " -f -4 | uniq
```

* print the expected count of folders
```
bash iter_table_partitions.sh --count "your_csv_file" "end_day" | wc -l
```

* count the folders on S3
```
aws --profile prod-bora s3 ls s3://zephyrus-ef4-prod-bora-migrate/ | wc -l
```

* when the counts are not equal find the missing ones with
```
bash iter_table_partitions.sh --count min_day.csv 20171004 | sed 's/ //' | sort >should.out
aws --profile prod-bora s3 ls s3://zephyrus-ef4-prod-bora-migrate/ | sed 's/.*PRE //' | sed 's/\///' | sort >is.out
diff is.out should.out
```

* Run the migration: transfer from S3 to GCS
```
gcloud config configurations activate prod-bora
gsutil ls gs://zephyrus-ef4-prod-bora-migrate/ | wc -l
```

* Drop and create the day-partitioned tables
> This will wipe all the data in the tables.
> *Make sure this is what you want*
```
# bigshift with --step drop
bash iter_table_partitions.sh "your_csv_file" >n.out 2>&1
```

* Run the migration: load
```
# bigshift with --step load
bash iter_table_partitions.sh "your_csv_file" "end_day" >n.out 2>&1
```

* count rows in partitions for Redshift tables
```
python3 db_count.py --rs --daily --count "your_project" "end_day"
```

* count rows in partitions for BigQuery tables
```
python3 db_count.py --bq --daily --count "your_project" "end_day"
```

* Validate the migration
```
python3 db_diff.py --verify "your_project"
```

