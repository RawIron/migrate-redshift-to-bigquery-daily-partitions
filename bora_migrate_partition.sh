# calls the bigshift tool
#   https://github.com/iconara/bigshift
# which was modified to support the migration of daily partitions
#   https://github.com/RawIron/bigshift/tree/support-daily-partitions
#
# required in environment
#   AWS_REGION
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#
# partition in format
#   20170930


tablename="$1"
partition="$2"
cutoff="$3"
bigshift_home="${HOME}/workspace/bigshift"
migrate_home="${HOME}/workspace/migrate_redshift_to_bigquery_daily_partitions"


cd ${bigshift_home}
bundle exec ./bin/bigshift --steps load \
    --rs-database ef4 --rs-schema bora --rs-table "$tablename" \
    --rs-credentials ~/.aws/rs_bora.yml \
    --s3-bucket zephyrus-ef4-prod-bora-migrate \
    --gcp-credentials ./etc/bora/gcp.json \
    --cs-bucket zephyrus-ef4-prod-bora-migrate --bq-dataset bora \
    --no-compression \
    --partition-day "$partition"
    --cutoff-day "$cutoff"
    --time-column "timestamp"
