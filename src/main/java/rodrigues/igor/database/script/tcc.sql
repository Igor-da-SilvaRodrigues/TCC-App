-- Igor da Silva Rodrigues

-- E1
-- Toda a estratégia é mapeada em apenas uma tabela. A chave primária da tabela representará os atributos identificadores das entidades genéricas e especializadas. Não será permitido um valor discriminador para o conjunto genérico.

create database if not exists tcc_e1;
use tcc_e1;

drop table if exists Pessoa;
drop table if exists Tipo;

create table if not exists Tipo(
    tipo varchar(2) primary key
);
insert into Tipo(tipo) values ('PF'),('PJ');

create table if not exists Pessoa(
    id varchar(36) primary key,
    nome varchar(100),
    cpf varchar(11), -- apenas numeros
    cnpj varchar(14),
    tipo varchar(2),
    foreign key (tipo) references Tipo(tipo)
);

-- -------------------------------------------------------------

-- E2
-- Toda a hierarquia é mapeada em apenas uma tabela. A chave primária da tabela representará os atributos identificadores das entidades genéricas e especializadas. Será permitido o uso de um valor discriminador para o conjunto genérico.

create database if not exists tcc_e2;
use tcc_e2;

drop table if exists Pessoa;
drop table if exists Tipo;

create table if not exists Tipo(
    tipo varchar(2) primary key
);
insert into Tipo(tipo) values ('PF'),('PJ'),('P');

create table if not exists Pessoa(
    id varchar(36) primary key,
    nome varchar(100),
    cpf varchar(11),
    cnpj varchar(14),
    tipo varchar(2),
    foreign key (tipo) references Tipo(tipo)
);

-- -------------------------------------------------------------

-- E3
-- Toda a hierarquia é mapeada em apenas uma tabela. Será permitido o uso de atributos discriminadores compostos através de um relacionamento n pra n com uma tabela contendo os valores existentes. Não será permitido um valor discriminador para o conjunto genérico.

create database if not exists tcc_e3;
use tcc_e3;

drop table if exists Pessoa_Tipo;
drop table if exists Pessoa;
drop table if exists Tipo;

create table if not exists Tipo(
    tipo varchar(2) primary key
);
insert into Tipo(tipo) values ('PF'), ('PJ');

create table if not exists Pessoa(
    id varchar(36) primary key,
    nome varchar(100),
    cpf varchar(11),
    cnpj varchar(14)
);

create table if not exists Pessoa_Tipo(
    id_Pessoa varchar(36),
    id_Tipo varchar(2),
    primary key (id_Pessoa, id_Tipo),
    foreign key (id_Pessoa) references Pessoa(id),
    foreign key (id_Tipo) references Tipo(tipo)
);

-- -------------------------------------------------------------

-- E4
-- Toda a hierarquia é mapeada em apenas uma tabela. Será permitido o uso de atributos discriminadores compostos. Será permitido o uso de um valor discriminador para o conjunto genérico.

create database if not exists tcc_e4;
use tcc_e4;

drop table if exists Pessoa_Tipo;
drop table if exists Pessoa;
drop table if exists Tipo;

create table if not exists Tipo(
    tipo varchar(2) primary key
);
insert into Tipo(tipo) values ('PF'), ('PJ'), ('P');

create table if not exists Pessoa(
    id varchar(36) primary key,
    nome varchar(100),
    cpf varchar(11),
    cnpj varchar(14)
);

create table if not exists Pessoa_Tipo(
    id_Pessoa varchar(36),
    id_Tipo varchar(2),
    primary key(id_Pessoa, id_Tipo),
    foreign key(id_Pessoa) references Pessoa(id),
    foreign key(id_Tipo) references Tipo(tipo)
);

-- -------------------------------------------------------------

-- E5
-- Uma tabela é criada para cada conjunto de entidades especializado. A chave primária das tabelas representará o atributo identificador da entidade genérica e da entidade especializada simultaneamente.

create database if not exists tcc_e5;
use tcc_e5;

drop table if exists PessoaFisica;
drop table if exists PessoaJuridica;

create table if not exists PessoaFisica(
    id varchar(36) primary key,
    nome varchar(100),
    cpf varchar(11)
);

create table if not exists PessoaJuridica(
    id varchar(36) primary key,
    nome varchar(100),
    cnpj varchar(14)
);

-- -------------------------------------------------------------

-- E6
-- Uma tabela é criada para cada conjunto de entidades na hierarquia. A relação entre as tabelas será feita com uma chave estrangeira nas tabelas especializadas.

create database if not exists tcc_e6;
use tcc_e6;

drop table if exists PessoaJuridica;
drop table if exists PessoaFisica;
drop table if exists Pessoa;


create table if not exists Pessoa(
    id varchar(36) primary key,
    nome varchar(100)
);

create table if not exists PessoaFisica(
    id_Pessoa varchar(36) primary key,
    cpf varchar(11),
    foreign key (id_Pessoa) references Pessoa(id)
);

create table if not exists PessoaJuridica(
    id_Pessoa varchar(36) primary key,
    cnpj varchar(14),
    foreign key (id_Pessoa) references Pessoa(id)
);