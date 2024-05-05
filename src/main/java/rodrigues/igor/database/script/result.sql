-- Igor da Silva Rodrigues
-- Este script cria o esquema que armazenará os resultados dos testes

create database if not exists result;
use result;

drop table if exists CreateTests;
create table if not exists CreateTests(
    E1 double,
    E2 double,
    E3 double,
    E4 double,
    E5 double,
    E6 double
);

drop table if exists SelectTests;
create table if not exists SelectTests(
    E1 double,
    E2 double,
    E3 double,
    E4 double,
    E5 double,
    E6 double
);

drop table if exists UpdateTests;
create table if not exists UpdateTests(
    E1 double,
    E2 double,
    E3 double,
    E4 double,
    E5 double,
    E6 double
);

drop table if exists DeleteTests;
create table if not exists DeleteTests(
    E1 double,
    E2 double,
    E3 double,
    E4 double,
    E5 double,
    E6 double
);