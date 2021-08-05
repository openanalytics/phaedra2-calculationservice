create schema if not exists calculations;

drop table if exists calculations.formula_execution_log;
drop table if exists calculations.formula;

create table if not exists calculations.formula
(
    id          bigserial,
    name        text      not null,
    description text,
    category    text      not null,
    formula     text      not null,
    language    text      not null,
    scope       integer   not null,
    created_by  text      not null,
    created_on  timestamp not null,
    updated_by  text      not null,
    updated_on  timestamp not null,
    primary key (id)
);

create table if not exists calculations.formula_execution_log
(
    id          bigserial,
    formula_id  bigint    not null,
    feature_id  bigint    not null,
    executed_by text      not null,
    executed_on timestamp not null,
    primary key (id),
    foreign key (formula_id) references calculations.formula (id)
);