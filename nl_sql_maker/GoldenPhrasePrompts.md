# Golden Phrase Review Prompts

- Generated: **2025-08-24T20:10:33Z**
- Artifacts: `out`
- Total NL candidates: **325** (batch size: 120; prompts: 3)

---

### Output format (return exactly YAML, nothing else):
```yaml
cases:
  - nl: "<copy one candidate exactly>"
    expect_ok: true   # or false
    reason: "short justification (type compatibility, ordering clause, etc.)"
```

**Guidance:**
- Mark `expect_ok: true` if the NL should bind & parse under our runtime (single FROM table).
- Mark `expect_ok: false` for clause-like constructs in SELECT (e.g., `order_by_asc`, `having`, `group_by`).
- `st_*` usually require geometry columns; `sum/avg/min/max/count` typically require numeric columns.
- Mixed tables in SELECT are usually **not** allowed (single FROM).
- When unsure, lean `true` but note the ambiguity in `reason`.

---

## Prompt 1 of 3

You are curating NL→SQL candidates. Review the list below and return **only** the YAML schema above.

**Candidates:**

1. select st_contains of boundaries, st_y of boundaries, and region_id from regions
2. select st_contains of boundaries, st_y of boundaries, region_id from regions
3. select st_contains of boundaries and st_y of boundaries and region_id from regions
4. select st_contains of regions.boundaries, st_y of regions.boundaries, and regions.region_id from regions
5. SELECT ST_CONTAINS OF BOUNDARIES, ST_Y OF BOUNDARIES, AND REGION_ID FROM REGIONS
6. select region_id, and name from regions
7. select region_id, name from regions
8. select region_id and name from regions
9. select regions.region_id, and regions.name from regions
10. SELECT REGION_ID, AND NAME FROM REGIONS
11. select boundaries and st_y of boundaries from regions
12. select boundaries, st_y of boundaries from regions
13. select boundaries, and st_y of boundaries from regions
14. select regions.boundaries and st_y of regions.boundaries from regions
15. SELECT BOUNDARIES AND ST_Y OF BOUNDARIES FROM REGIONS
16. select regions.region_id and regions.name from regions
17. SELECT REGION_ID AND NAME FROM REGIONS
18. select boundaries, concat of name from regions
19. select boundaries and concat of name from regions
20. select boundaries, and concat of name from regions
21. select regions.boundaries, concat of regions.name from regions
22. SELECT BOUNDARIES, CONCAT OF NAME FROM REGIONS
23. select name, length of boundaries from regions
24. select name and length of boundaries from regions
25. select name, and length of boundaries from regions
26. select regions.name, length of regions.boundaries from regions
27. SELECT NAME, LENGTH OF BOUNDARIES FROM REGIONS
28. select boundaries and length of region_id from regions
29. select boundaries, length of region_id from regions
30. select boundaries, and length of region_id from regions
31. select regions.boundaries and length of regions.region_id from regions
32. SELECT BOUNDARIES AND LENGTH OF REGION_ID FROM REGIONS
33. select boundaries from regions
34. select regions.boundaries from regions
35. SELECT BOUNDARIES FROM REGIONS
36. select name, boundaries from regions
37. select name and boundaries from regions
38. select name, and boundaries from regions
39. select regions.name, regions.boundaries from regions
40. SELECT NAME, BOUNDARIES FROM REGIONS
41. select boundaries, and st_distance of boundaries from regions
42. select boundaries, st_distance of boundaries from regions
43. select boundaries and st_distance of boundaries from regions
44. select regions.boundaries, and st_distance of regions.boundaries from regions
45. SELECT BOUNDARIES, AND ST_DISTANCE OF BOUNDARIES FROM REGIONS
46. select region_id and region_id and region_id from regions
47. select region_id, region_id, region_id from regions
48. select region_id, region_id, and region_id from regions
49. select regions.region_id and regions.region_id and regions.region_id from regions
50. SELECT REGION_ID AND REGION_ID AND REGION_ID FROM REGIONS
51. select st_geometrytype of boundaries, length of name, region_id from regions
52. select st_geometrytype of boundaries and length of name and region_id from regions
53. select st_geometrytype of boundaries, length of name, and region_id from regions
54. select st_geometrytype of regions.boundaries, length of regions.name, regions.region_id from regions
55. SELECT ST_GEOMETRYTYPE OF BOUNDARIES, LENGTH OF NAME, REGION_ID FROM REGIONS
56. select st_area of boundaries, st_length of boundaries, and region_id from regions
57. select st_area of boundaries, st_length of boundaries, region_id from regions
58. select st_area of boundaries and st_length of boundaries and region_id from regions
59. select st_area of regions.boundaries, st_length of regions.boundaries, and regions.region_id from regions
60. SELECT ST_AREA OF BOUNDARIES, ST_LENGTH OF BOUNDARIES, AND REGION_ID FROM REGIONS
61. select avg of region_id from regions
62. select avg of regions.region_id from regions
63. SELECT AVG OF REGION_ID FROM REGIONS
64. select st_intersects of boundaries, st_geometrytype of boundaries, name from regions
65. select st_intersects of boundaries and st_geometrytype of boundaries and name from regions
66. select st_intersects of boundaries, st_geometrytype of boundaries, and name from regions
67. select st_intersects of regions.boundaries, st_geometrytype of regions.boundaries, regions.name from regions
68. SELECT ST_INTERSECTS OF BOUNDARIES, ST_GEOMETRYTYPE OF BOUNDARIES, NAME FROM REGIONS
69. select st_union of boundaries from regions
70. select st_union of regions.boundaries from regions
71. SELECT ST_UNION OF BOUNDARIES FROM REGIONS
72. select concat of region_id from regions
73. select concat of regions.region_id from regions
74. SELECT CONCAT OF REGION_ID FROM REGIONS
75. select regions.name, and regions.boundaries from regions
76. SELECT NAME, AND BOUNDARIES FROM REGIONS
77. select avg of region_id, st_contains of boundaries, name from regions
78. select avg of region_id and st_contains of boundaries and name from regions
79. select avg of region_id, st_contains of boundaries, and name from regions
80. select avg of regions.region_id, st_contains of regions.boundaries, regions.name from regions
81. SELECT AVG OF REGION_ID, ST_CONTAINS OF BOUNDARIES, NAME FROM REGIONS
82. select regions.region_id, regions.name from regions
83. SELECT REGION_ID, NAME FROM REGIONS
84. select length of region_id, st_union of boundaries, and boundaries from regions
85. select length of region_id, st_union of boundaries, boundaries from regions
86. select length of region_id and st_union of boundaries and boundaries from regions
87. select length of regions.region_id, st_union of regions.boundaries, and regions.boundaries from regions
88. SELECT LENGTH OF REGION_ID, ST_UNION OF BOUNDARIES, AND BOUNDARIES FROM REGIONS
89. select region_id from regions
90. select regions.region_id from regions
91. SELECT REGION_ID FROM REGIONS
92. select region_id, and st_buffer of boundaries from regions
93. select region_id, st_buffer of boundaries from regions
94. select region_id and st_buffer of boundaries from regions
95. select regions.region_id, and st_buffer of regions.boundaries from regions
96. SELECT REGION_ID, AND ST_BUFFER OF BOUNDARIES FROM REGIONS
97. select st_y of boundaries from regions
98. select st_y of regions.boundaries from regions
99. SELECT ST_Y OF BOUNDARIES FROM REGIONS
100. select length of name, st_union of boundaries, and boundaries from regions
101. select length of name, st_union of boundaries, boundaries from regions
102. select length of name and st_union of boundaries and boundaries from regions
103. select length of regions.name, st_union of regions.boundaries, and regions.boundaries from regions
104. SELECT LENGTH OF NAME, ST_UNION OF BOUNDARIES, AND BOUNDARIES FROM REGIONS
105. select boundaries, and cast of boundaries from regions
106. select boundaries, cast of boundaries from regions
107. select boundaries and cast of boundaries from regions
108. select regions.boundaries, and cast of regions.boundaries from regions
109. SELECT BOUNDARIES, AND CAST OF BOUNDARIES FROM REGIONS
110. select boundaries, boundaries from regions
111. select boundaries and boundaries from regions
112. select boundaries, and boundaries from regions
113. select regions.boundaries, regions.boundaries from regions
114. SELECT BOUNDARIES, BOUNDARIES FROM REGIONS
115. select st_touches of boundaries from regions
116. select st_touches of regions.boundaries from regions
117. SELECT ST_TOUCHES OF BOUNDARIES FROM REGIONS
118. select st_length of quantity from sales
119. select st_length of sales.quantity from sales
120. SELECT ST_LENGTH OF QUANTITY FROM SALES

---

## Prompt 2 of 3

You are curating NL→SQL candidates. Review the list below and return **only** the YAML schema above.

**Candidates:**

1. select price and product_name and sale_id from sales
2. select price, product_name, sale_id from sales
3. select price, product_name, and sale_id from sales
4. select sales.price and sales.product_name and sales.sale_id from sales
5. SELECT PRICE AND PRODUCT_NAME AND SALE_ID FROM SALES
6. select extract of quantity, quantity, sale_date from sales
7. select extract of quantity and quantity and sale_date from sales
8. select extract of quantity, quantity, and sale_date from sales
9. select extract of sales.quantity, sales.quantity, sales.sale_date from sales
10. SELECT EXTRACT OF QUANTITY, QUANTITY, SALE_DATE FROM SALES
11. select sale_date, and sale_date from sales
12. select sale_date, sale_date from sales
13. select sale_date and sale_date from sales
14. select sales.sale_date, and sales.sale_date from sales
15. SELECT SALE_DATE, AND SALE_DATE FROM SALES
16. select sales.user_id and sales.user_id from sales
17. select sales.user_id, sales.user_id from sales
18. select sales.user_id, and sales.user_id from sales
19. SELECT SALES.USER_ID AND SALES.USER_ID FROM SALES
20. select st_x of sales.user_id from sales
21. SELECT ST_X OF SALES.USER_ID FROM SALES
22. select sale_id and sale_id from sales
23. select sale_id, sale_id from sales
24. select sale_id, and sale_id from sales
25. select sales.sale_id and sales.sale_id from sales
26. SELECT SALE_ID AND SALE_ID FROM SALES
27. select st_intersects of quantity from sales
28. select st_intersects of sales.quantity from sales
29. SELECT ST_INTERSECTS OF QUANTITY FROM SALES
30. select sale_id from sales
31. select sales.sale_id from sales
32. SELECT SALE_ID FROM SALES
33. select product_name and st_intersects of product_name from sales
34. select product_name, st_intersects of product_name from sales
35. select product_name, and st_intersects of product_name from sales
36. select sales.product_name and st_intersects of sales.product_name from sales
37. SELECT PRODUCT_NAME AND ST_INTERSECTS OF PRODUCT_NAME FROM SALES
38. select sale_id and quantity and sale_id from sales
39. select sale_id, quantity, sale_id from sales
40. select sale_id, quantity, and sale_id from sales
41. select sales.sale_id and sales.quantity and sales.sale_id from sales
42. SELECT SALE_ID AND QUANTITY AND SALE_ID FROM SALES
43. select st_x of price from sales
44. select st_x of sales.price from sales
45. SELECT ST_X OF PRICE FROM SALES
46. select quantity, and product_name from sales
47. select quantity, product_name from sales
48. select quantity and product_name from sales
49. select sales.quantity, and sales.product_name from sales
50. SELECT QUANTITY, AND PRODUCT_NAME FROM SALES
51. select quantity and sale_date and price from sales
52. select quantity, sale_date, price from sales
53. select quantity, sale_date, and price from sales
54. select sales.quantity and sales.sale_date and sales.price from sales
55. SELECT QUANTITY AND SALE_DATE AND PRICE FROM SALES
56. select price, and cast of product_name from sales
57. select price, cast of product_name from sales
58. select price and cast of product_name from sales
59. select sales.price, and cast of sales.product_name from sales
60. SELECT PRICE, AND CAST OF PRODUCT_NAME FROM SALES
61. select product_name from sales
62. select sales.product_name from sales
63. SELECT PRODUCT_NAME FROM SALES
64. select sales.user_id and st_contains of sales.price from sales
65. select sales.user_id, st_contains of sales.price from sales
66. select sales.user_id, and st_contains of sales.price from sales
67. SELECT SALES.USER_ID AND ST_CONTAINS OF SALES.PRICE FROM SALES
68. select sale_date and max of price from sales
69. select sale_date, max of price from sales
70. select sale_date, and max of price from sales
71. select sales.sale_date and max of sales.price from sales
72. SELECT SALE_DATE AND MAX OF PRICE FROM SALES
73. select sales.user_id, st_contains of sales.quantity from sales
74. select sales.user_id and st_contains of sales.quantity from sales
75. select sales.user_id, and st_contains of sales.quantity from sales
76. SELECT SALES.USER_ID, ST_CONTAINS OF SALES.QUANTITY FROM SALES
77. select sale_id, and sale_date from sales
78. select sale_id, sale_date from sales
79. select sale_id and sale_date from sales
80. select sales.sale_id, and sales.sale_date from sales
81. SELECT SALE_ID, AND SALE_DATE FROM SALES
82. select sale_date, st_union of quantity from sales
83. select sale_date and st_union of quantity from sales
84. select sale_date, and st_union of quantity from sales
85. select sales.sale_date, st_union of sales.quantity from sales
86. SELECT SALE_DATE, ST_UNION OF QUANTITY FROM SALES
87. select quantity, and length of sale_id from sales
88. select quantity, length of sale_id from sales
89. select quantity and length of sale_id from sales
90. select sales.quantity, and length of sales.sale_id from sales
91. SELECT QUANTITY, AND LENGTH OF SALE_ID FROM SALES
92. select st_distance of sales.user_id from sales
93. SELECT ST_DISTANCE OF SALES.USER_ID FROM SALES
94. select sale_date from sales
95. select sales.sale_date from sales
96. SELECT SALE_DATE FROM SALES
97. select sale_id, length of product_name from sales
98. select sale_id and length of product_name from sales
99. select sale_id, and length of product_name from sales
100. select sales.sale_id, length of sales.product_name from sales
101. SELECT SALE_ID, LENGTH OF PRODUCT_NAME FROM SALES
102. select st_x of product_name from sales
103. select st_x of sales.product_name from sales
104. SELECT ST_X OF PRODUCT_NAME FROM SALES
105. select sales.user_id, and sales.price from sales
106. select sales.user_id, sales.price from sales
107. select sales.user_id and sales.price from sales
108. SELECT SALES.USER_ID, AND SALES.PRICE FROM SALES
109. select last_login from users
110. select users.last_login from users
111. SELECT LAST_LOGIN FROM USERS
112. select st_centroid of users.location and users.balance and users.user_id from users
113. select st_centroid of users.location, users.balance, users.user_id from users
114. select st_centroid of users.location, users.balance, and users.user_id from users
115. SELECT ST_CENTROID OF USERS.LOCATION AND USERS.BALANCE AND USERS.USER_ID FROM USERS
116. select is_active from users
117. select users.is_active from users
118. SELECT IS_ACTIVE FROM USERS
119. select users.balance, count of users.user_id from users
120. select users.balance and count of users.user_id from users

---

## Prompt 3 of 3

You are curating NL→SQL candidates. Review the list below and return **only** the YAML schema above.

**Candidates:**

1. select users.balance, and count of users.user_id from users
2. SELECT USERS.BALANCE, COUNT OF USERS.USER_ID FROM USERS
3. select balance, and age from users
4. select balance, age from users
5. select balance and age from users
6. select users.balance, and users.age from users
7. SELECT BALANCE, AND AGE FROM USERS
8. select users.user_id from users
9. SELECT USERS.USER_ID FROM USERS
10. select users.user_id and min of users.location from users
11. select users.user_id, min of users.location from users
12. select users.user_id, and min of users.location from users
13. SELECT USERS.USER_ID AND MIN OF USERS.LOCATION FROM USERS
14. select age and age from users
15. select age, age from users
16. select age, and age from users
17. select users.age and users.age from users
18. SELECT AGE AND AGE FROM USERS
19. select last_login and age from users
20. select last_login, age from users
21. select last_login, and age from users
22. select users.last_login and users.age from users
23. SELECT LAST_LOGIN AND AGE FROM USERS
24. select location from users
25. select users.location from users
26. SELECT LOCATION FROM USERS
27. select users.user_id, and users.balance from users
28. select users.user_id, users.balance from users
29. select users.user_id and users.balance from users
30. SELECT USERS.USER_ID, AND USERS.BALANCE FROM USERS
31. select age from users
32. select users.age from users
33. SELECT AGE FROM USERS
34. select username from users
35. select users.username from users
36. SELECT USERNAME FROM USERS
37. select st_crosses of location, age, and location from users
38. select st_crosses of location, age, location from users
39. select st_crosses of location and age and location from users
40. select st_crosses of users.location, users.age, and users.location from users
41. SELECT ST_CROSSES OF LOCATION, AGE, AND LOCATION FROM USERS
42. select location, location, and is_active from users
43. select location, location, is_active from users
44. select location and location and is_active from users
45. select users.location, users.location, and users.is_active from users
46. SELECT LOCATION, LOCATION, AND IS_ACTIVE FROM USERS
47. select balance from users
48. select users.balance from users
49. SELECT BALANCE FROM USERS
50. select is_active and st_buffer of location from users
51. select is_active, st_buffer of location from users
52. select is_active, and st_buffer of location from users
53. select users.is_active and st_buffer of users.location from users
54. SELECT IS_ACTIVE AND ST_BUFFER OF LOCATION FROM USERS
55. select location, and last_login from users
56. select location, last_login from users
57. select location and last_login from users
58. select users.location, and users.last_login from users
59. SELECT LOCATION, AND LAST_LOGIN FROM USERS
60. select st_transform of location from users
61. select st_transform of users.location from users
62. SELECT ST_TRANSFORM OF LOCATION FROM USERS
63. select last_login and location and is_active from users
64. select last_login, location, is_active from users
65. select last_login, location, and is_active from users
66. select users.last_login and users.location and users.is_active from users
67. SELECT LAST_LOGIN AND LOCATION AND IS_ACTIVE FROM USERS
68. select is_active and st_spatial_index of location from users
69. select is_active, st_spatial_index of location from users
70. select is_active, and st_spatial_index of location from users
71. select users.is_active and st_spatial_index of users.location from users
72. SELECT IS_ACTIVE AND ST_SPATIAL_INDEX OF LOCATION FROM USERS
73. select balance and st_contains of location from users
74. select balance, st_contains of location from users
75. select balance, and st_contains of location from users
76. select users.balance and st_contains of users.location from users
77. SELECT BALANCE AND ST_CONTAINS OF LOCATION FROM USERS
78. select username, st_buffer of location from users
79. select username and st_buffer of location from users
80. select username, and st_buffer of location from users
81. select users.username, st_buffer of users.location from users
82. SELECT USERNAME, ST_BUFFER OF LOCATION FROM USERS
83. select min of location from users
84. select min of users.location from users
85. SELECT MIN OF LOCATION FROM USERS

---
