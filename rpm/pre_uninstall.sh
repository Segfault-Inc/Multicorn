default_version=$(grep default_version $(pg_config --sharedir)/extension/multicorn.control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

rm $(pg_config --sharedir)/extension/multicorn--${default_version}.sql
