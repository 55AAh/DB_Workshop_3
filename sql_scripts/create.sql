DROP TABLE Empl_Manager;
DROP TABLE Manager;
DROP TABLE Empl_TermReason;
DROP TABLE TermReason;
DROP TABLE Empl;
DROP TABLE Human_ZipCode;
DROP TABLE ZipCode;
DROP TABLE StateInfo;
DROP TABLE Human;
DROP TABLE MrtStatus;
DROP TABLE Company;
DROP TABLE Department;
DROP TABLE EmplPos;
DROP TABLE EmplStatus;
DROP TABLE PerfScore;

CREATE TABLE company (
    company_id  NUMBER NOT NULL,
    name        VARCHAR2(100)
);

ALTER TABLE company ADD CONSTRAINT company_pk PRIMARY KEY ( company_id );

CREATE TABLE department (
    department_id  NUMBER NOT NULL,
    name           VARCHAR2(100)
);

ALTER TABLE department ADD CONSTRAINT department_pk PRIMARY KEY ( department_id );

CREATE TABLE empl (
    human_human_id            NUMBER NOT NULL,
    company_company_id        NUMBER NOT NULL,
    department_department_id  NUMBER NOT NULL,
    emplpos_position_id       NUMBER NOT NULL,
    emplstatus_estatus_id     NUMBER NOT NULL,
    perfscore_pscore_id       NUMBER NOT NULL,
    empl_satisfaction         NUMBER(1)
);

ALTER TABLE empl
    ADD CONSTRAINT empl_pk PRIMARY KEY ( human_human_id,
                                         company_company_id,
                                         department_department_id,
                                         emplpos_position_id );

CREATE TABLE empl_manager (
    empl_human_human_id            NUMBER NOT NULL,
    empl_company_company_id        NUMBER NOT NULL,
    empl_department_department_id  NUMBER NOT NULL,
    empl_emplpos_position_id       NUMBER NOT NULL,
    manager_manager_id             NUMBER NOT NULL
);

ALTER TABLE empl_manager
    ADD CONSTRAINT empl_manager_pk PRIMARY KEY ( empl_human_human_id,
                                                 empl_company_company_id,
                                                 empl_department_department_id,
                                                 empl_emplpos_position_id,
                                                 manager_manager_id );

CREATE TABLE empl_termreason (
    empl_human_human_id            NUMBER NOT NULL,
    empl_company_company_id        NUMBER NOT NULL,
    empl_department_department_id  NUMBER NOT NULL,
    empl_emplpos_position_id       NUMBER NOT NULL,
    termreason_treason_id          NUMBER NOT NULL
);

ALTER TABLE empl_termreason
    ADD CONSTRAINT empl_termreason_pk PRIMARY KEY ( empl_human_human_id,
                                                    empl_company_company_id,
                                                    empl_department_department_id,
                                                    empl_emplpos_position_id,
                                                    termreason_treason_id );

CREATE TABLE emplpos (
    position_id  NUMBER NOT NULL,
    name         VARCHAR2(100)
);

ALTER TABLE emplpos ADD CONSTRAINT emplpos_pk PRIMARY KEY ( position_id );

CREATE TABLE emplstatus (
    estatus_id  NUMBER NOT NULL,
    status      VARCHAR2(30)
);

ALTER TABLE emplstatus ADD CONSTRAINT emplstatus_pk PRIMARY KEY ( estatus_id );

CREATE TABLE human (
    human_id              NUMBER NOT NULL,
    mrtstatus_mstatus_id  NUMBER NOT NULL
);

ALTER TABLE human ADD CONSTRAINT human_pk PRIMARY KEY ( human_id );

CREATE TABLE human_zipcode (
    human_human_id   NUMBER NOT NULL,
    zipcode_zipcode  NUMBER NOT NULL
);

ALTER TABLE human_zipcode ADD CONSTRAINT human_zipcode_pk PRIMARY KEY ( human_human_id,
                                                                        zipcode_zipcode );

CREATE TABLE manager (
    manager_id  NUMBER NOT NULL,
    name        VARCHAR2(100)
);

ALTER TABLE manager ADD CONSTRAINT manager_pk PRIMARY KEY ( manager_id );

CREATE TABLE mrtstatus (
    mstatus_id  NUMBER NOT NULL,
    status      VARCHAR2(30)
);

ALTER TABLE mrtstatus ADD CONSTRAINT mrtstatus_pk PRIMARY KEY ( mstatus_id );

CREATE TABLE perfscore (
    pscore_id  NUMBER NOT NULL,
    score      VARCHAR2(30)
);

ALTER TABLE perfscore ADD CONSTRAINT perfscore_pk PRIMARY KEY ( pscore_id );

CREATE TABLE stateinfo (
    state_id  NUMBER NOT NULL,
    name      VARCHAR2(30)
);

ALTER TABLE stateinfo ADD CONSTRAINT stateinfo_pk PRIMARY KEY ( state_id );

CREATE TABLE termreason (
    treason_id  NUMBER NOT NULL,
    reason      VARCHAR2(40)
);

ALTER TABLE termreason ADD CONSTRAINT termreason_pk PRIMARY KEY ( treason_id );

CREATE TABLE zipcode (
    zipcode             NUMBER NOT NULL,
    stateinfo_state_id  NUMBER NOT NULL
);

ALTER TABLE zipcode ADD CONSTRAINT zipcode_pk PRIMARY KEY ( zipcode );

ALTER TABLE empl
    ADD CONSTRAINT empl_company_fk FOREIGN KEY ( company_company_id )
        REFERENCES company ( company_id );

ALTER TABLE empl
    ADD CONSTRAINT empl_department_fk FOREIGN KEY ( department_department_id )
        REFERENCES department ( department_id );

ALTER TABLE empl
    ADD CONSTRAINT empl_emplpos_fk FOREIGN KEY ( emplpos_position_id )
        REFERENCES emplpos ( position_id );

ALTER TABLE empl
    ADD CONSTRAINT empl_emplstatus_fk FOREIGN KEY ( emplstatus_estatus_id )
        REFERENCES emplstatus ( estatus_id );

ALTER TABLE empl
    ADD CONSTRAINT empl_human_fk FOREIGN KEY ( human_human_id )
        REFERENCES human ( human_id );

ALTER TABLE empl_manager
    ADD CONSTRAINT empl_manager_empl_fk FOREIGN KEY ( empl_human_human_id,
                                                      empl_company_company_id,
                                                      empl_department_department_id,
                                                      empl_emplpos_position_id )
        REFERENCES empl ( human_human_id,
                          company_company_id,
                          department_department_id,
                          emplpos_position_id );

ALTER TABLE empl_manager
    ADD CONSTRAINT empl_manager_manager_fk FOREIGN KEY ( manager_manager_id )
        REFERENCES manager ( manager_id );

ALTER TABLE empl
    ADD CONSTRAINT empl_perfscore_fk FOREIGN KEY ( perfscore_pscore_id )
        REFERENCES perfscore ( pscore_id );

ALTER TABLE empl_termreason
    ADD CONSTRAINT empl_termreason_empl_fk FOREIGN KEY ( empl_human_human_id,
                                                         empl_company_company_id,
                                                         empl_department_department_id,
                                                         empl_emplpos_position_id )
        REFERENCES empl ( human_human_id,
                          company_company_id,
                          department_department_id,
                          emplpos_position_id );

ALTER TABLE empl_termreason
    ADD CONSTRAINT empl_termreason_termreason_fk FOREIGN KEY ( termreason_treason_id )
        REFERENCES termreason ( treason_id );

ALTER TABLE human
    ADD CONSTRAINT human_mrtstatus_fk FOREIGN KEY ( mrtstatus_mstatus_id )
        REFERENCES mrtstatus ( mstatus_id );

ALTER TABLE human_zipcode
    ADD CONSTRAINT human_zipcode_human_fk FOREIGN KEY ( human_human_id )
        REFERENCES human ( human_id );

ALTER TABLE human_zipcode
    ADD CONSTRAINT human_zipcode_zipcode_fk FOREIGN KEY ( zipcode_zipcode )
        REFERENCES zipcode ( zipcode );

ALTER TABLE zipcode
    ADD CONSTRAINT zipcode_stateinfo_fk FOREIGN KEY ( stateinfo_state_id )
        REFERENCES stateinfo ( state_id );