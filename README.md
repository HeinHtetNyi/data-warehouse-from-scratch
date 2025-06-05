### Data Warehouse Structure
The Data Warehouse is structured into three layers - Bronze, Silver, and Gold.
This warehouse is built on DuckDB which is in-process OLAP simliar to SQLite.


### Bronze Layer
In this layer, raw data is inserted to bronze schema.
- Transformation ❌
- Data Modeling ❌

Purpose: To keep the original data as it is for backup.


### Silver Layer
In this layer, data is processed to get into shape that we want.
- Transformation ✅
- Data Modeling ❌


### Gold Layer
In this layer, data is integrated to fit in Star Schema. We define dimension and fact tables.
- Data Modeling (Star Schema) ✅
