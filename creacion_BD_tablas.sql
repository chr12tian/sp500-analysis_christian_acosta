CREATE DATABASE SP500;
GO

USE SP500;
GO

CREATE TABLE CompanyProfiles (
    Symbol VARCHAR(10) PRIMARY KEY,
    Company VARCHAR(100),
    Sector VARCHAR(50),
    Headquarters VARCHAR(100),
    FechaFundada VARCHAR(10)
);
GO

CREATE TABLE Companies (
    Date DATE,
    Symbol VARCHAR(10),
    close FLOAT,
    PRIMARY KEY (Date, Symbol),
    FOREIGN KEY (Symbol) REFERENCES CompanyProfiles(Symbol)
);
GO
