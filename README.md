# pgtxdb

[![Build Status](https://travis-ci.org/achiku/pgtxdb.svg?branch=master)](https://travis-ci.org/achiku/pgtxdb)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/achiku/pgtxdb/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/achiku/pgtxdb)](https://goreportcard.com/report/github.com/achiku/pgtxdb)

## Description

Single transaction sql driver for Golang x PostgreSQL. This is almost clone of [go-txdb](https://github.com/DATA-DOG/go-txdb) with a bit of PostgreSQL tweeks.

- When `conn.Bigin()` is called, this library executes `SAVEPOINT pgtxdb_xxx;` instead of actually begins transaction. 
- `tx.Commit()` does nothing.
- `ROLLBACK TO SAVEPOINT pgtxdb_xxx;` will be executed upon `tx.Rollback()` call so that it can emulate transaction rollback.
- Above features enable us to emulate multiple transactions in one test case.


## Run test

Make sure PostgreSQL is running.

```
create database pgtxdbtest;
create user pgtxdbtest;
```
