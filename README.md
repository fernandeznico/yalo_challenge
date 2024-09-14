# Yalo Challenge

## Task 1 | Data Lake | Iowa Liquor Retail Sales 

### Data Lineage
```mermaid
flowchart LR;
    subgraph BigQuery
        iowa_liquor_sales[[fa:fa-table iowa_liquor_sales]]
    end
```

### Data Layers
```mermaid
flowchart LR;
    RAW-->STAGING-->ANALYTICS
```

 * RAW
 * STAGING
 * ANALYTICS

## Task 3 | Chuck Norris Jokes

 * [Chuck Norris Jokes DAG code →](dags/dag_chuck_norris_jokes/chuck_norris_jokes.py)