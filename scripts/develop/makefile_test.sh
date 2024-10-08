#!/bin/bash
echo "123"
export APP_ID="weops_saas"
export APP_TOKEN="9b5df2e5-f638-4146-baf5-22251a527691"
export RUN_VER="open"
export BK_URL="http://paas.weops.com"
export BK_PAAS_HOST="http://paas.weops.com"
mysql -h localhost -uroot -e "CREATE DATABASE IF NOT EXISTS \`$APP_ID\` default charset utf8 COLLATE utf8_general_ci;"

usage() { echo "Usage: [-m test module] [-e exclude tag]" 1>&2; exit 1; }

if [[ -d .cover ]]; then
    rm -rf .cover
fi

INCLUDE_PATH="home_application/*"
OMIT_PATH="*/migrations/*,*/tests/*"

exclude_tag=''
module=''

while getopts ":e:m:" opt; do
    case ${opt} in
      e )
        exclude_tag=$OPTARG
        ;;
      m )
        module=$OPTARG
        ;;
      * )
        usage
        ;;
    esac
done

# random test database name
DB_NAME="test_$RANDOM"
sed -i.bak "s/test_db/$DB_NAME/g" config/dev.py && rm config/dev.py.bak

revert_db() { sed -i.bak "s/$DB_NAME/test_db/g" config/dev.py && rm config/dev.py.bak; exit 1; }

coverage erase
coverage run --include=$INCLUDE_PATH --omit=$OMIT_PATH ./manage.py test $module --exclude-tag=$exclude_tag || revert_db
coverage html -d .cover
coverage report --include=$INCLUDE_PATH --omit=$OMIT_PATH || revert_db

sed -i.bak "s/$DB_NAME/test_db/g" config/dev.py && rm config/dev.py.bak

echo "234"