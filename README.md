# advanced-db_assignment3

# Project Documentation:

---

## 1. Architecture Diagram

The data pipeline follows the industry-standard **Medallion Architecture**, separating concerns into three distinct layers to transform raw, volatile data into high-value, optimized business assets.

```
  [ Scheduled CSV Extracts ]
   (Day 1, Day 2, Day 3, etc.)
               |
               v  [ COPY INTO / Batch Append ]
  +-------------------------------------------------+
  |                  BRONZE LAYER                   |
  |  - Table: `raw_orders`                         |
  |  - Raw, unaltered source data in Delta format   |
  |  - Includes technical metadata (input file name)|
  +-------------------------------------------------+
               |
               v  [ MERGE INTO / Idiomatic Upsert ]
  +-------------------------------------------------+
  |                  SILVER LAYER                   |
  |  - Table: `silver_orders`                       |
  |  - Deduplicated grain (Order ID + Product ID)  |
  |  - Validated data types & schema enforcement    |
  +-------------------------------------------------+
               |
       +-------+-------+-----------------------+
       |               |                       |
       v               v                       v
 [ GROUP BY / SUM ]  [ GROUP BY / SUM ]  [ ROW_NUMBER() / LAG() ]
+------------------+ +------------------+ +----------------------+
|    GOLD LAYER    | |    GOLD LAYER    | |   ANALYTICS LAYER    |
| `gold_sales_...` | | `gold_customer_` | | (Analytical Views)   |
| - Daily sales    | |  metrics`        | | - Top 5 Products     |
| - Regional sales | | - Revenue & count| | - Monthly Trends     |
| - Category sales | |   of unique orders| | - Running Totals     |
+------------------+ +------------------+ +----------------------+
```

## 2. Data Flow Description

Data progresses through the pipeline in a unidirectional, automated sequence designed to support incremental ingestion:

1. **Ingestion (Bronze):** Daily operational sales logs (`day_1.csv`, `day_2.csv`, etc.) are pulled from cloud landing storage. The data is appended to the Delta table `raw_orders` exactly as it arrives, along with a `_metadata` column to guarantee full lineage and historical traceability.
2. **Refinement & Deduplication (Silver):** The Silver pipeline processes changes incrementally using a SQL `MERGE INTO` statement. It evaluates incoming data against the existing `silver_orders` dataset based on a composite business key. If a matching record is found, it is updated (handling modifications or backfilled data); if it is new, it is inserted.
3. **Aggregation & Serving (Gold & Analytics):** Cleaned data from `silver_orders` is used to build aggregate tables (`gold_*`) tailored for rapid dashboarding. Advanced window functions (`ROW_NUMBER()`, `LAG()`, `SUM() OVER()`) run directly on top of the Silver layer to extract trends, rankings, and running totals for business stakeholders.

## 3. Data Quality Rules (DQ)

To ensure the integrity of business reporting, the Silver layer enforces the following data quality conditions:

- **Entity Integrity:** The combination of `order_id` and `product_name` must be unique. Duplicates introduced by file-reloading or manual entry errors are automatically resolved during the `MERGE` operation.
- **Schema & Type Enforcement:** Text fields are safely cast, financial metrics (such as `sales`) are explicitly converted to `DOUBLE`, and string timestamps are parsed into valid `DATE` objects to allow safe down-stream calculations like `DATE_TRUNC`.
- **Completeness (Not Null):** Critical structural dimensions (`order_id`, `customer_name`, `region`, `category`) are verified to ensure they do not contain missing values (`NULL`), ensuring that grouped metrics do not drop rows or misrepresent actual data distributions.

## 4. Assumptions

The pipeline design is built upon the following operational assumptions:

- **Multi-Item Orders:** A single `order_id` does *not* represent a unique row in the primary source file; a separate row is generated for each individual item purchased within that transaction. Therefore, calculation of total individual sales actions requires using `COUNT(DISTINCT order_id)`.
- **Out-of-Order Backfills:** Files delivered on later dates (e.g., Day 3) may contain adjustments or late entries for earlier transactions (e.g., Day 1). The system assumes that updating historical records on a matching business key is preferred over throwing an error or creating a duplicate.
- **Standardized Dimensions:** Dimensional attributes such as `region` and `category` are assumed to be cleaned and unified at the source level (e.g., case-sensitive alignment), meaning no advanced fuzzy text-matching is required prior to joining or grouping.

## 5. Limitations

- **Memory Constraints for Broadcast Joins:** The explicit performance optimization applied in Task 6 using the `/*+ BROADCAST(r) */` hint relies entirely on the fact that the dimension tables (e.g., `gold_sales_region`) are small enough to fit comfortably into the memory of every working node. If these dimensions scale past the physical memory barrier, this operation will trigger an `OutOfMemoryError`.
- **System Policy Restrictions (Unity Catalog):** Because the pipeline operates inside a secured, shared environment governed by Databricks Unity Catalog, global runtime variables (such as `spark.sql.autoBroadcastJoinThreshold`) are strictly immutable. Execution plans must therefore be manually optimized and guided via inline query hints rather than global configurations.
- **Lack of Historical Versioning (SCD Type 1 Behavior):** The Silver layer updates changed records in-place. If a customer changes their geographic `region`, the `MERGE` query replaces the historical value, which means the pipeline currently lacks Slowly Changing Dimension (SCD Type 2) tracking to report where that user lived at the exact moment of an older purchase.
