# go-mysql-sphinx

Connects to MySQL/MariaDB via the replication protocol, and sends updates to [Sphinx](https://sphinxsearch.com/).

## Example quick start

    docker-compose up

## Usage

1. Install [Go](https://golang.org/doc/install)

2. Install [dep](https://golang.github.io/dep/docs/installation.html)

3. Build

        $ make

4. Configure MySQL/MariaDB

    - `binlog_format` must be `ROW`
    - `binlog_row_image` must be `FULL` (otherwise you may not be able to determine which documents should be updated)

5. Configure Sphinx

    - index `sync_state` should exist and have the following structure

            type = rt
            rt_attr_string = binlog_name
            rt_attr_uint   = binlog_position
            rt_attr_string = gtid
            rt_attr_string = flavor

    - for every RT-index there should be a plain index with `_plain` suffix
    - if an index is partitioned, then its parts should have the same schema and names that end with `_part_0`, `_part_1`, etc.
    - `searchd` should expect all index files to be in the same folder (i.e. `path` of each index should have the same dirname)

6. Configure go-mysql-sphinx

See example config file [river.toml](./etc/river.toml).

7. Run

        $ ./bin/go-mysql-sphinx [-config config-file]

