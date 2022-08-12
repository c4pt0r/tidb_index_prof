# tidb_index_prof

## Usage 

`./tidb_index_prof -u <dbname> -p <dbpass> -H host -P <port> -l <log level>`

output: sql query summary in last 30minutes, how many times each index is used (will also output non used indexes)
for more detail: https://docs.pingcap.com/tidb/dev/statement-summary-tables


## Example


```
create table t(a varchar(255) primary key, b varchar(255), c int, key b(b), key c(c));
insert into t values('a', 'b', 1);
insert into t values('aa', 'bb', 2);
insert into t values('aaa', 'bbbb', 3);
insert into t values('aaaaa', 'bbbbb', 4);
select * from t;
select * from t where a='a';
select * from t where a='aa';
select * from t where a='aaa' or c = 4;

$./tidb_index_prof -u root -H 127.0.0.1 -P 4000 -db test
--- Index usage stat:
{
  "t": {
    "t:PRIMARY": 3,
    "t:b": 0,
    "t:c": 1,
    "t:primary": 1
  }
}
--- Full table scan samples:
[
  {
    "digest_text": "select * from `t`",
    "digest": "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
    "table_names": [
      "test.t"
    ],
    "used_indexes": null,
    "count": 1,
    "first_seen": "2022-08-11T23:59:23Z",
    "last_seen": "2022-08-11T23:59:23Z"
  }
]

```
