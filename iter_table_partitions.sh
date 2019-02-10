# reads a file with
#   tablename,start_day
#
# uses end_day to generate
#   tablename,start_day,start_day+1,..,end_day


day_diff() {
    local start="$1"
    local end="$2"

    sec_diff=$(( $(date +%s -d "$end") - $(date +%s -d "$start") ))
    day_diff=$(( ${sec_diff} / 86400 ))

    echo $day_diff
}


gen_day_series() {
    local start="$1"
    local days="$2"

    for i in $(seq 0 ${days}); do
      date -d "$start $i days" +%Y-%m-%d
    done
}


too_old=200
migrator="bash ostro_migrate_partition.sh"


migrate_partitions() {
    local tablename="$1"
    local start_day="$2"
    local end_day="$3"

    if [[ -z $end_day ]]; then
        days=0
    else
        days=$(day_diff $start_day $end_day)
    fi

    if [[ -z "$start_day" ]]; then
        echo "${tablename}::no start day" >&2
        return 0
    elif [[ $days -gt $too_old ]]; then
        echo "${tablename}::start day is too far in the past ${start_day}" >&2
        return 0
    fi

    for partition in $(gen_day_series $start_day $days); do
        $migrator $tablename $(echo "$partition" | tr -d '-') &
    done
}


runner() {
    local csv_in="$1"
    local end_day="$2"
    local cutoff_day="$3"

    while IFS=',' read -ra tuple || [[ -n "$tuple" ]]; do
        tablename=${tuple[0]}
        start_day=${tuple[1]}
        migrate_partitions "$tablename" "$start_day" "$end_day" "$cutoff_day"
	wait
    done < "$csv_in"
}


main() {
    # parse command line arguments
    local positional=()
    local opt_count=0
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            --count)
            opt_count=1
            shift
            ;;
            *) # save it in an array for later
            positional+=("$1")
            shift
            ;;
        esac
    done
    # restore positional parameters
    set -- "${positional[@]}"

    csv_in="$1"
    end_day="$2"
    cutoff_day="$3"

    if [[ ${opt_count} == 1 ]]; then
        migrator="echo"
    fi

    runner $csv_in $end_day $cutoff_day
}


[[ "$0" == "$BASH_SOURCE" ]] && main "$@" || true
