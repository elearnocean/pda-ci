# DacMini
Include existing DB objects (or scripts) to build small custom SQL Server DacPac 

---
DacMini is a utility that builds a custom DacPac from a small number of existing objects in a SQL server (mostly tables and views).

A file is created with a list of objects to be imported from one or more SQL Servers and/or SQL script files.
The tool works through this list, extracts them from SQL (or supplied files) and then builds a DacPac.  

This tool grew from a need to use SSDT database projects in existing large databases (usually Data Warehouses) in which our team only developed a small number of objects. Multiple other teams were not using database projects and we found the need to reference some other objects that we did not own, for example DimDate and DimCustomer tables (DW dimensions).

We'd extract, build and reference this mini DacPac so we could build, deploy and test our project without having to keep the full database dacpac reference up to date, and worry about other referenced objects changing and/or build issues trying to update other objects outside our scope, based on other teams work and changes.

This may or may not be a good idea, but we found that it worked for us.  Tool grew organically - forgive the hatchet job on the code.

