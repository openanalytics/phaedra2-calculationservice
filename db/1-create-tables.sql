CREATE SCHEMA IF NOT EXISTS calculations;

DROP TABLE IF EXISTS calculations.formula_execution_log;
DROP TABLE IF EXISTS calculations.formula;
-- drop table calculations.formula CASCADE ;

CREATE TYPE calculations.scope AS ENUM ('PLATE', 'FEATURE', 'WELL', 'SUB_WELL');
CREATE CAST (CHARACTER VARYING AS calculations.scope) WITH INOUT AS IMPLICIT;
CREATE TYPE calculations.category AS ENUM ('CALCULATION', 'HIT_CALLING', 'OUTLIER_DETECTION', 'POLISHING');
CREATE CAST (CHARACTER VARYING AS calculations.category) WITH INOUT AS IMPLICIT;


CREATE TABLE IF NOT EXISTS calculations.formula
(
    id          bigserial,
    name        text                  NOT NULL,
    description text,
    category    calculations.category NOT NULL,
    formula     text                  NOT NULL,
    language    text                  NOT NULL,
    scope       calculations.scope    NOT NULL,
    created_by  text                  NOT NULL,
    created_on  timestamp             NOT NULL,
    updated_by  text                  NOT NULL,
    updated_on  timestamp             NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS calculations.formula_execution_log
(
    id          bigserial,
    formula_id  bigint    NOT NULL,
    feature_id  bigint    NOT NULL,
    executed_by text      NOT NULL,
    executed_on timestamp NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (formula_id) REFERENCES calculations.formula (id)
);
