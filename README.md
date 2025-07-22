# ideas

This revised response focuses specifically on query language actions, their type-based limitations, user-defined metadata influences, and a tiered approach to complexity, along with a conceptual workflow and keyword categorization, as requested.

## Query Action Types, Metadata Limitations, and Workflow for Natural Language Generation

This document outlines a framework for mapping SQL and PostGIS query actions to natural language expressions, considering data types and user-defined metadata. The goal is to provide a structured approach for generating a grammar that supports natural language interaction with databases.

### 1\. Conceptual Design Principles

  * **Tokens:** Field names, values, actions, and keywords are all considered tokens that combine to form expressions.
  * **Context Tracking:** The system will track context (e.g., action type, field type, specific function being applied) to inform behavior, such as value conversion (e.g., "high intensity" to a numeric range).
  * **Type and Metadata Limitation:** Query actions are inherently limited by the data types of the columns they operate on. User-provided metadata further refines these limitations or enables specific functionalities.
  * **Tiered Complexity:** Queries are categorized into two tiers: "Simple & Common" and "Moderately Complex & Common," to manage the scope of natural language generation.
  * **Atomic Expressions:** The aim is to define atomic natural language expressions for columns, actions, and keywords to minimize combinatorial explosion, while still allowing for composition.

### 2\. Query Action Types (Type-Limited)

This section presents query action types for both SQL and PostGIS, categorized by their inherent type applicability.

#### 2.1 SQL Query Action Types

**Tier 1: Simple & Common Queries**

These are fundamental data retrieval and filtering operations.

| Action Category           | Action Type (SQL Equivalent)           | Applicable Data Types                               | Keywords/Operators (within rule) | Examples (Conceptual)                                       |
| :------------------------ | :------------------------------------- | :-------------------------------------------------- | :------------------------------- | :---------------------------------------------------------- |
| **Direct Value Retrieval** | Equality (`=`)                         | All types (Numeric, Text, Date/Time, Boolean)       | `=`                              | `column = value`, `column IS NULL`                          |
|                           | Inequality (`!=`, `<>`)                | All types (Numeric, Text, Date/Time, Boolean)       | `!=`, `<>`                       | `column != value`                                           |
| **Range Filtering** | Less Than (`<`)                        | Numeric, Date/Time                                  | `<`                              | `column < value`                                            |
|                           | Greater Than (`>`)                     | Numeric, Date/Time                                  | `>`                              | `column > value`                                            |
|                           | Less Than or Equal (`<=`)              | Numeric, Date/Time                                  | `<=`                             | `column <= value`                                           |
|                           | Greater Than or Equal (`>=`)           | Numeric, Date/Time                                  | `>=`                             | `column >= value`                                           |
| **Set Membership** | IN (`IN`)                              | All types (List of discrete values)                 | `IN`                             | `column IN (value1, value2)`                                |
| **Pattern Matching** | Like (`LIKE`, `ILIKE`)                 | Text                                                | `LIKE`, `ILIKE`                  | `column LIKE '%pattern%'`                                   |
| **Null Check** | Is Null (`IS NULL`, `IS NOT NULL`)     | All types                                           | `IS NULL`, `IS NOT NULL`         | `column IS NULL`                                            |
| **Aggregation (Basic)** | Count (`COUNT`)                        | All types (Counts rows, or non-null values)         | `COUNT`                          | `COUNT(column)`, `COUNT(*)`                                 |
|                           | Sum (`SUM`)                            | Numeric                                             | `SUM`                            | `SUM(column)`                                               |
|                           | Average (`AVG`)                        | Numeric                                             | `AVG`                            | `AVG(column)`                                               |
|                           | Minimum (`MIN`)                        | Numeric, Date/Time, Text (alphabetical)             | `MIN`                            | `MIN(column)`                                               |
|                           | Maximum (`MAX`)                        | Numeric, Date/Time, Text (alphabetical)             | `MAX`                            | `MAX(column)`                                               |

**Tier 2: Moderately Complex & Common Queries**

These involve more sophisticated filtering, grouping, and ordering.

| Action Category         | Action Type (SQL Equivalent)         | Applicable Data Types                               | Keywords/Operators (within rule) | Examples (Conceptual)                                        |
| :---------------------- | :----------------------------------- | :-------------------------------------------------- | :------------------------------- | :----------------------------------------------------------- |
| **Range (Inclusive)** | Between (`BETWEEN`)                  | Numeric, Date/Time                                  | `BETWEEN ... AND ...`            | `column BETWEEN value1 AND value2`                           |
| **Ordering** | Order By (`ORDER BY ASC`, `ORDER BY DESC`) | All types (for sorting)                             | `ASC`, `DESC`                    | `ORDER BY column ASC`                                        |
| **Grouping** | Group By (`GROUP BY`)                | All types (for categorical grouping)                | `GROUP BY`                       | `GROUP BY column`                                            |
| **Aggregated Filtering** | Having (`HAVING`)                    | Numeric (results of aggregation)                    | `=`, `<`, `>`, etc.              | `HAVING COUNT(column) > value`                               |
| **Distinct Values** | Distinct (`DISTINCT`)                | All types                                           | `DISTINCT`                       | `SELECT DISTINCT column`                                     |
| **Limiting Results** | Limit (`LIMIT`)                      | N/A (applies to result set)                         | `LIMIT`                          | `LIMIT N`                                                    |
| **Date/Time Extraction** | Year, Month, Day, Hour, etc. (`EXTRACT`) | Date/Time                                           | `EXTRACT(unit FROM column)`      | `EXTRACT(YEAR FROM date_column) = 2023`                      |
| **String Manipulation** | Length (`LENGTH`), Concat (`CONCAT`) | Text                                                | `LENGTH`, `CONCAT`               | `LENGTH(column) > N`, `CONCAT(col1, col2)`                   |
| **Type Casting** | Cast (`CAST`)                        | All types (conversion between compatible types)     | `CAST(... AS ...)`               | `CAST(numeric_column AS TEXT)`                               |

#### 2.2 PostGIS Query Action Types

PostGIS actions often require `GEOMETRY` or `GEOGRAPHY` data types. Metadata can specify the SRID (Spatial Reference ID) or the specific geometry type (Point, LineString, Polygon, MultiPolygon, etc.), which further refines applicability.

**Tier 1: Simple & Common Spatial Queries**

These focus on basic spatial relationships and properties.

| Action Category            | Action Type (PostGIS Function)                     | Applicable Data Types                     | Keywords/Operators (within rule) | Examples (Conceptual)                                                |
| :------------------------- | :------------------------------------------------- | :---------------------------------------- | :------------------------------- | :------------------------------------------------------------------- |
| **Spatial Relationships** | Intersects (`ST_Intersects`)                       | `GEOMETRY`, `GEOGRAPHY` (of any type)     | N/A                              | `ST_Intersects(geom_col, input_geom)`                                |
|                            | Within (`ST_Within`)                               | `GEOMETRY`, `GEOGRAPHY` (of any type)     | N/A                              | `ST_Within(geom_col, input_polygon)`                                 |
|                            | Contains (`ST_Contains`)                           | `GEOMETRY`, `GEOGRAPHY` (of any type)     | N/A                              | `ST_Contains(geom_col_polygon, input_point)`                         |
| **Spatial Measurement** | Distance (`ST_Distance`)                           | `GEOMETRY`, `GEOGRAPHY`                   | N/A                              | `ST_Distance(geom_col, input_geom) < N`                              |
|                            | Length (`ST_Length`)                               | `GEOMETRY` (LineString, MultiLineString)  | N/A                              | `ST_Length(line_geom_col) > N`                                       |
|                            | Area (`ST_Area`)                                   | `GEOMETRY` (Polygon, MultiPolygon)        | N/A                              | `ST_Area(polygon_geom_col) > N`                                      |
| **Geometry Accessors** | X Coordinate (`ST_X`)                              | `GEOMETRY` (Point)                        | N/A                              | `ST_X(point_geom_col) > N`                                           |
|                            | Y Coordinate (`ST_Y`)                              | `GEOMETRY` (Point)                        | N/A                              | `ST_Y(point_geom_col) < N`                                           |
|                            | Geometry Type (`ST_GeometryType`)                  | `GEOMETRY`, `GEOGRAPHY`                   | N/A                              | `ST_GeometryType(geom_col) = 'ST_Point'`                             |
| **Bounding Box** | Bounding Box Intersection (`&&`)                   | `GEOMETRY` (Operator)                     | `&&`                             | `geom_col && input_box` (checks if bounding boxes intersect)         |

**Tier 2: Moderately Complex & Common Spatial Queries**

These involve more advanced spatial operations like buffering, unions, or specific projections.

| Action Category            | Action Type (PostGIS Function)      | Applicable Data Types                      | Keywords/Operators (within rule) | Examples (Conceptual)                                        |
| :------------------------- | :---------------------------------- | :----------------------------------------- | :------------------------------- | :----------------------------------------------------------- |
| **Geometry Processing** | Buffer (`ST_Buffer`)                | `GEOMETRY`                                 | N/A                              | `ST_Intersects(geom_col, ST_Buffer(input_point, distance))` |
|                            | Union (`ST_Union`)                  | `GEOMETRY` (Set of geometries)             | N/A                              | `ST_Union(geom_col_group)`                                   |
|                            | Transform (`ST_Transform`)          | `GEOMETRY` (for SRID conversion)           | N/A                              | `ST_Transform(geom_col, target_srid)`                        |
|                            | Centroid (`ST_Centroid`)            | `GEOMETRY` (Polygon, MultiPolygon)         | N/A                              | `ST_X(ST_Centroid(polygon_col))`                             |
|                            | Simplify (`ST_Simplify`)            | `GEOMETRY` (LineString, Polygon)           | N/A                              | `ST_Simplify(geom_col, tolerance)`                           |
| **Advanced Relationships** | Touches (`ST_Touches`)              | `GEOMETRY`                                 | N/A                              | `ST_Touches(geom_col1, geom_col2)`                           |
|                            | Crosses (`ST_Crosses`)              | `GEOMETRY` (LineString/Polygon interaction) | N/A                              | `ST_Crosses(line_col, polygon_col)`                          |
| **Nearest Neighbor** | K-Nearest Neighbor (`<->` operator) | `GEOMETRY` (Operator)                      | `<->`                            | `geom_col <-> input_point ORDER BY geom_col <-> input_point LIMIT 1` (Often used with ORDER BY/LIMIT) |

### 3\. User-Defined Metadata for Function Limitation/Expansion

Metadata provides critical context beyond just the database's inherent data type. It acts as a set of rules to *filter* or *enable* specific actions for a column.

  * **Limitation (Filtering Out Actions):**
      * **`id` tag for Integers:** An `INT` column tagged as `id` (e.g., `customer_id`, `order_number`) would typically **exclude** comparative operations (`<`, `>`, `<=`, `>=`), arithmetic operations (`+`, `-`, `*`, `/`), `SUM`, `AVG`, `ST_Area`, `ST_Length`, `LIKE`. It would primarily be used for `Equality`, `IN`, `COUNT`, `MIN`, `MAX` (to find the lowest/highest ID), `GROUP BY`, `ORDER BY`.
      * **`flag` or `status` tag for Booleans/Enums:** A `BOOLEAN` or `VARCHAR` (for 'active', 'inactive') column tagged as `flag` or `status` would primarily be limited to `Equality`, `IN`, `IS NULL`, and `COUNT` (e.g., "count active users").
      * **`raw_text` tag for Strings:** A `VARCHAR` column containing unstructured text (e.g., `comments`, `description`) might **exclude** `SUM`, `AVG`, direct `Equality` (unless an exact match is expected for short codes), and heavily emphasize `LIKE`/`ILIKE`, `LENGTH`, and potentially more advanced text-search functions if available.
      * **`secure` tag:** This could block any direct retrieval of values, only allowing `COUNT` or `IS NULL`.
  * **Expansion (Enabling Specific Actions):**
      * **`latitude` / `longitude` tags for Numerics:** A `DECIMAL` or `FLOAT` column tagged specifically as `latitude` or `longitude` would *not* automatically map to all spatial functions. Instead, it would *enable* its use in `ST_Point` constructor functions when combined with a corresponding `longitude`/`latitude` column, leading to the creation of a `GEOMETRY(POINT)` object for subsequent spatial operations. For example, `ST_Distance(ST_MakePoint(longitude_col, latitude_col), input_point)`.
      * **`geometry` / `geography` type directly:** Columns with native PostGIS types (`GEOMETRY`, `GEOGRAPHY`, `POINT`, `LINESTRING`, `POLYGON`, etc.) would automatically map to the functions outlined in the PostGIS section, *unless* a specific metadata tag restricts them (e.g., a `GEOMETRY(POINT)` column tagged `administrative_center` might only support `ST_Distance` and `ST_X/Y`, but not `ST_Area` or `ST_Length`).
      * **`srid_4326` or `srid_26918` tags:** For `GEOMETRY` columns, an SRID tag ensures that relevant spatial functions assume or require that specific projection, influencing how distance or area calculations are performed and enabling functions like `ST_Transform`.

### 4\. Categories of Keywords

Keywords are essential for constructing and combining query expressions. They can be broadly categorized as:

  * **In-Rule Keywords/Operators:** These are intrinsic to a specific query action and appear *within* the definition of that action. They often have direct natural language mappings.
      * Examples: `=`, `<`, `>`, `<=`, `>=`, `!=`, `<>`, `LIKE`, `IN`, `BETWEEN ... AND ...`, `IS NULL`, `ASC`, `DESC`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `EXTRACT`, `LENGTH`, `CAST`, `&&` (PostGIS bounding box operator).
  * **Compositional Keywords (Joining Rules):** These keywords combine or modify entire query expressions or clauses, forming more complex queries. These are typically handled *outside* the atomic action rules but combine the results of multiple rules.
      * **Logical Operators:** `AND`, `OR`, `NOT`
      * **Grouping/Nesting:** Parentheses `( )` (implies logical grouping or subqueries)
      * **Aggregation/Grouping:** `GROUP BY`, `HAVING`
      * **Ordering/Limiting:** `ORDER BY`, `LIMIT`
      * **Selection:** `SELECT` (as a general starting point for query)
      * **Joins:** `JOIN`, `ON` (though the prompt asks to avoid overly complicated joins, simple equijoins might be considered later for connecting related entities).

### 5\. Basic Workflow for Rule Generation

This workflow outlines the process from schema input to the generation of natural language-mappable rules.

1.  **Schema and Metadata Ingestion:**

      * **Input:** Database Schema (table names, column names, SQL data types) and User-Defined Metadata (e.g., `{"column_name": {"tag": "id", "description": "unique identifier for customers"}, "location_geom": {"tag": "spatial_data", "geometry_type": "POINT", "srid": 4326, "description": "Geographical location of a point of interest"}}`).
      * **Process:** Parse schema and metadata into an internal representation.

2.  **Column-Specific Action Filtering/Expansion:**

      * For each column:
          * Identify its base SQL data type (e.g., `INT`, `VARCHAR`, `GEOMETRY`).
          * Based on the base type, identify the *default set* of applicable query actions from the lists in Section 2.
          * Apply user-defined metadata rules (Section 3):
              * If a `limitation` tag exists (e.g., `id`, `flag`), remove non-applicable actions from the default set.
              * If an `expansion` tag exists (e.g., `latitude`/`longitude`, specific `geometry_type`), add relevant functions (e.g., `ST_Point` constructor, specific PostGIS functions) or refine parameter types for existing functions.
      * **Output:** For each column, a tailored list of valid query actions (from Tier 1 and Tier 2) it can participate in, along with their expected input types (e.g., `column_name: [ {action: "Equality", input_type: "value"}, {action: "COUNT", input_type: "none"} ]`).

3.  **Natural Language Expression Generation (LLM Prompts):**

      * **Column Explanations:**
          * **Prompt:** "Given the column `{column_name}` with data type `{data_type}` and description `{description}`, what are various natural language terms or phrases a user might use to refer to this column? Provide atomic expressions and common synonyms. Consider its role as `{metadata_tag}` if applicable."
          * **Example Output:** `customer_id` -\> "customer ID", "customer number", "ID of the customer", "which customer"
          * **Output:** A list of natural language tokens/phrases for each column.
      * **Action Explanations (per applicable column):**
          * **Prompt:** "Given the column `{column_name}` (type: `{data_type}`, description: `{description}`) and the query action `{action_type}` (e.g., 'Equality', 'ST\_Distance'), generate concise natural language phrases for using this action with this column. Emphasize how the action operates on the column. Provide examples where appropriate. Include common synonyms for the action's keyword/operator (e.g., '=', '\<', 'BETWEEN')."
          * **Examples:**
              * `customer_id` + `Equality`: "customer ID is [value]", "customer ID equals [value]", "for customer [value]"
              * `order_total` + `GreaterThan`: "order total more than [value]", "orders over [value]", "order total is greater than [value]"
              * `location_geom` + `ST_Distance` (to a given point): "distance from [point] to location", "how far location is from [point]", "locations within [distance] of [point]"
          * **Output:** A list of natural language templates/phrases for each valid `(column, action)` pair.
      * **Keyword Explanations:**
          * **Prompt (In-Rule Keywords):** "Generate natural language terms for the operator/keyword `{keyword}` (e.g., '=', '\<', 'BETWEEN'). Consider its typical usage in queries."
          * **Example Output:** `=` -\> "is", "equals", "of", "whose"
          * **Prompt (Compositional Keywords):** "Generate natural language phrases for combining statements using `{keyword}` (e.g., 'AND', 'OR', 'NOT'). Consider how they link multiple conditions."
          * **Example Output:** `AND` -\> "and", "in addition to", "both...and..."
          * **Output:** A list of natural language tokens/phrases for each relevant keyword/operator.

4.  **Grammar Construction (Directed Graph/Lark):**

      * **Process:**
          * Represent the schema, actions, and generated natural language phrases as a directed graph.
          * Nodes: Represent columns, action types, keywords, and abstract syntactic categories (e.g., `filter_expression`, `column_reference`, `value`).
          * Edges: Represent how these elements combine according to SQL/PostGIS syntax and the derived NL phrases. For example, an edge from `column_reference` to `equality_action` to `value`.
          * Map this graph directly to Lark grammar rules. Each NL template from step 3.3 becomes a production rule.
          * **Example (Conceptual Lark Rule based on NL):**
            ```
            ?query: "show" column_nl "where" filter_expression
            column_nl: "customer id" | "order total" | "location"
            filter_expression: column_nl "is" value_nl
                             | column_nl "more than" value_nl
                             | location_nl "within" distance_nl "of" point_nl

            // value_nl, distance_nl, point_nl would have their own rules based on expected input types
            ```
      * **Output:** An initial Lark grammar representing the possible natural language queries.

5.  **Iterative Abstraction and Refinement (LLM Loop):**

      * **Test Case Generation:** Use the LLM to generate diverse natural language query examples based on the initial grammar and schema.
      * **Parsing and SQL Generation:** Attempt to parse these NL examples using the current Lark grammar and generate corresponding SQL.
      * **Feedback Loop:**
          * **False Positives:** If an NL phrase parses and generates SQL, but the SQL is incorrect or nonsensical given the intent, identify this as a false positive. **Prompt LLM:** "The grammar currently allows '{NL\_phrase}' which translates to '{Incorrect\_SQL}'. This is incorrect for column '{column\_name}' and action '{action\_type}'. How can the grammar rule for '{relevant\_rule\_name}' be refined to disallow this or guide it to the correct interpretation? Provide specific changes to the rule."
          * **New Phrases (Coverage Gaps):** If a desired NL phrase (from user testing or LLM generation) *fails* to parse, identify this as a coverage gap. **Prompt LLM:** "The natural language phrase '{NL\_phrase}' should correspond to the SQL '{Desired\_SQL}'. However, it does not currently parse with the grammar. Based on the schema for '{column\_name}' and the action '{action\_type}', how can a new rule or modification to an existing rule be added to the grammar to support this phrase? Ensure it remains atomic where possible."
          * **Abstraction:** After multiple iterations, prompt the LLM to find common patterns and abstract rules (e.g., combining `is`, `equals`, `of` into a single `EQUALITY_KEYWORD` rule).
      * **Output:** A progressively more robust and accurate Lark grammar that maps natural language to SQL/PostGIS queries.

This structured approach, focusing on atomic query actions and leveraging metadata, provides a clear path for building a natural language interface that is both powerful and manageable.
