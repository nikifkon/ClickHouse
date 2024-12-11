#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function perform()
{
    local test_id=$1
    local query=$2

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query"
    code=$?

    if [ "$code" -eq 0 ]; then
        for f in "${CLICKHOUSE_TMP}"/*
        do
            echo "file: $(basename "$f")"
            sort < "$f"
        done
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}"/*
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS \`${CLICKHOUSE_TEST_NAME}\`;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE \`${CLICKHOUSE_TEST_NAME}\` (a String, b UInt64, c UInt64) Engine=Memory;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO \`${CLICKHOUSE_TEST_NAME}\` VALUES ('x', 1, 1), ('x', 2, 2), ('x', 3, 3), ('y', 1, 4), ('y', 2, 5), ('y', 3, 6);"

perform "simple__identifier" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{a}' PARTITION BY a;"

perform "simple__expr_with_alias" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{mod}' PARTITION BY b % 2 as mod;"
perform "simple__expr_without_alias" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{modulo(b, 2)}' PARTITION BY modulo(b, 2);"
perform "simple__const" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' PARTITION BY 42;"

perform "simple__all_keys" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{c}_{b}_{a}' PARTITION BY a, b, c;"
perform "simple__can_reuse" "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{a}_{a}' PARTITION BY a;"

perform "escape__outside" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/outside_\{ {42} \}' PARTITION BY 42"
perform "escape__inside" "SELECT 42 as \` {42} \` INTO OUTFILE '${CLICKHOUSE_TMP}/inside_{ \{42\} }' PARTITION BY \` {42} \`"
perform "escape__heredoc" "SELECT 42 INTO OUTFILE \$heredoc\$${CLICKHOUSE_TMP}/heredoc_\{{42}\}\$heredoc\$ PARTITION BY 42"

touch "${CLICKHOUSE_TMP}/{42}"
perform "no_existing_check_for_pattern_itself" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' PARTITION BY 42;"

echo 42 > "${CLICKHOUSE_TMP}/42"
perform "simple_append" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' APPEND PARTITION BY 42;"

echo 42 > "${CLICKHOUSE_TMP}/42"
perform "simple_truncate" "SELECT 42 INTO OUTFILE '${CLICKHOUSE_TMP}/{42}' TRUNCATE PARTITION BY 42;"

echo "perform file exist test"

touch "${CLICKHOUSE_TMP}/y_2"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM \`${CLICKHOUSE_TEST_NAME}\` INTO OUTFILE '${CLICKHOUSE_TMP}/{a}_{b}' PARTITION BY a, b;" 2>&1 | grep -Fc "File exists"
rm "${CLICKHOUSE_TMP}"/*

echo "perform wrong template test"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP}/outfile' PARTITION BY a;" 2>&1 | grep -Fc "must use all keys"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP}/outfile_{}' PARTITION BY a;" 2>&1 | grep -Fc "unexpected column name"

${CLICKHOUSE_CLIENT} --query "SELECT 1 as a INTO OUTFILE '${CLICKHOUSE_TMP}/outfile_{b}' PARTITION BY a;" 2>&1 | grep -Fc "unexpected column name"

