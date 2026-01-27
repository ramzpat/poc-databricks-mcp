# Databricks Resources Summary for Lead Generation

## Overview
This document provides a comprehensive summary of the Databricks data sources accessible through the MCP (Model Context Protocol) lead generation server configured for the POC (Proof of Concept) environment.

**Key Note**: This MCP server is designed for audience sizing and lead generation via aggregated metrics only. No raw data is returned. All results are aggregated values (COUNT, SUM, AVG, MIN, MAX) for privacy-safe audience estimation.

---

## Catalog Structure

### Primary Catalog
- **Catalog Name**: `int_pipeline_poc`
- **Primary Schema**: `poc_mcp`
- **Access Pattern**: Via metadata and aggregation tools only

---

## Available Data Views for Audience Sizing

**Total Views: 8** - All available for aggregated audience sizing and lead generation

### View Inventory Summary

| View Name | Type | Key Use Case (Aggregated) | Partition | Grain |
|-----------|------|---------------------------|-----------|-------|
| vw_customer360_current | Customer Profile | Current audience count & segmentation | None | Current State |
| vw_customer360 | Customer Profile | Historical audience trends | Monthly (par_month) | Monthly State |
| vw_customer360_snapshot | Customer Profile | Temporal audience analysis | Daily (par_day) | Daily Snapshot |
| vw_fact_cdr_geo_agg_hour_v2 | Location/CDR | Geographic audience metrics | Daily + Hourly | Hourly Events |
| vw_custom_detail_beha_cellsite_usage_hour_daily | Behavior/Location | Location-based audience | Daily + Hourly | Hourly Behavior |
| vw_fact_seq_agg_hour | Sequence/Event | Event-based audience | Daily + Hourly | Hourly Sequences |
| vw_fact_seq_agg_day | Sequence/Event | Daily audience segments | Daily (par_day) | Daily Sequences |
| vw_fact_seq_agg_month | Sequence/Event | Monthly audience trends | Monthly (par_month) | Monthly Sequences |

## Accessing Data for Lead Generation

### Available MCP Tools

This lead generation MCP server provides the following tools for audience sizing:

**Metadata Tools** (data structure exploration):
- `list_catalogs`: Available catalogs
- `list_schemas(catalog)`: Schemas within a catalog
- `list_tables(catalog, schema)`: Tables/views in a schema
- `table_metadata(catalog, schema, table)`: Column information and statistics
- `partition_info(catalog, schema, table)`: Partition structure and stats

**Audience Sizing Tools** (no raw data returned):
- `approx_count(catalog, schema, table, predicate?)`: Get approximate row count with optional WHERE clause for audience sizing
- `aggregate_metric(catalog, schema, table, metric_type, metric_column, predicate?)`: Calculate aggregated metrics (COUNT, SUM, AVG, MIN, MAX)

### Example Workflow
1. Use `table_metadata` to explore column names and types in `vw_customer360_current`
2. Use `approx_count` with predicates like `demo_age_fact_v2_final_age_num > 25 AND beha_mobile_usage_active_period_weekly_flag = 1` to estimate audience size
3. Use `aggregate_metric` to calculate statistics like average data usage or count of high-value segments
4. Export conditions to generate Jupyter notebooks for internal DS team to analyze actual data on Databricks

---

## View Details for Audience Sizing

### 1. vw_customer360_current
**Type**: Current Customer 360 View (Latest Snapshot)
**Partition**: None (current data only)

#### Purpose
Real-time snapshot of the most recent customer 360 data representing the latest customer state.

#### Key Features
- Same comprehensive schema as vw_customer360
- Represents the current state without historical versioning
- Updated with latest customer profile data as of 2026-01-22
- Contains all 400+ customer attributes

#### Use Cases
- Real-time customer segmentation
- Current customer lookups
- Active campaign targeting
- Immediate customer profiling

---

### 2. vw_customer360
**Type**: Customer 360 View (Current Data)
**Partition**: Monthly (`par_month`)

#### Purpose
Real-time customer view combining customer demographics, behavior, interests, and service engagement data.

#### Key Features
- **Customer Identifiers**: msisdn (phone), uid (unique user ID), ban_num (billing account number)
- **Account Status**: activated_flag, subs_type (prepay/postpay), product_mix
- **Customer Lifecycle**: cust_aging_grp (customer tenure groups)
- **Consent Management**: consent_tuc (consent tracking data in JSON format)

#### Data Categories Included
1. **Demographics** (~20 columns)
   - Age (demo_age_fact_v2_final_age_num)
   - Gender (demo_gender_v1_gender_bin)
   - Device Info (demo_handset_monthly_clean_os_cat, device_type)
   - Payment Profile (demo_payment_monthly_payment_group_cat)
   - Life Stage (demo_lifestage_v2_lifestage_grp_cat)
   - Affluence (demo_affluence_grid_v1_affluence_group_bin)

2. **Behavioral Data** (~50+ columns)
   - Mobile Usage Patterns (beha_mobile_usage_active_period_weekly_*)
   - Voice & Data Usage (beha_mobile_calling_usage_monthly_*, beha_tran_mobile_data_usage_v1_*)
   - TrueID/OTT Activity (is_trueid_mau, is_android_box_mau)
   - Traveler Segments (beha_domestic_traveler_v1_travel_segment_cat)
   - Transportation Usage (beha_transportation_rail_bts_rider_v1, beha_bts_rider_v2_bts_rides_frequency_num)
   - Banking & Digital Services (beha_bank_app_usage_v1, beha_credit_card_app_usage_v1)

3. **Interest/Content Preferences** (~200+ columns)
   - Standard Interest Labels (inte_standard_label_*)
   - Content Interests (inte_entertainment_*, inte_sports_*, inte_fashion_*, etc.)
   - Financial Interests (inte_finance_*, inte_banking_*, inte_insurance_*)
   - Lifestyle Interests (inte_lifestyle_*, inte_health_*, inte_education_*)
   - Business Interests (inte_business_*, inte_marketing_*)
   - Vertical-Specific Audiences (inte_sb_* - segmented business audiences)

4. **Geographic Data** (~20 columns)
   - Resident Location (geog_resident_location_*)
   - Work Location (geog_work_location_*)
   - Location Affinity (geog_shp_* - shopping pattern locations)
   - Points of Interest (geog_poi_* - frequented locations)
   - Destination Analysis (geog_destination_location_v1_destination_no_num)

5. **Service Engagement** (~30 columns)
   - Digital Service Usage (inte_office_*, inte_online_learning_*)
   - Entertainment Streaming (inte_entertainment_*)
   - E-commerce Behavior (inte_ecommerce_*)
   - App Usage Patterns (beha_*_app_usage_v1)

#### Sample Record Count
- Recent monthly partition: 5+ rows per query
- Data is continuously updated with timestamps (created_datetime, updated_datetime)

#### Example Data Points
- Age range: 18-50+ years
- Data usage: 0-40+ GB monthly
- Gender: male, female, unknown
- Product mix: a2-pretmh, c9-posttmh, etc.
- Subscription types: prepay, postpay
- Affluence segments: low, medium, high
- TrueID MAU indicators: Yes/No
- Blocklist status: Yes/No

---

### 3. vw_customer360
**Type**: Customer 360 View (Monthly Data)
**Partition**: Monthly (`par_month`)

#### Purpose
Primary customer 360 view with monthly partitioning for efficient querying of historical data by month.

#### Key Features
- Comprehensive customer attributes (300+ columns)
- Partitioned by month (YYYYMM format, e.g., 202509)
- Tracks current and recent monthly customer states
- Useful for monthly reporting and trend analysis

---

### 4. vw_customer360_snapshot
**Type**: Customer 360 Snapshot (Historical Snapshot Data)
**Partition**: Daily (`par_day`)

#### Purpose
Point-in-time snapshots of customer data for historical analysis and trend tracking.

#### Key Features
- Similar schema to vw_customer360 but with daily partitioning
- Captures customer state at specific dates (e.g., 2025-06-01)
- Includes persona classifications (primary_persona, secondary_persona, tertiary_persona)
- Useful for:
  - Trend analysis
  - Cohort analysis over time
  - Customer state transitions
  - Historical behavior tracking

#### Persona Classification
- **primary_persona**: Primary customer segment (e.g., "reward_seeker")
- **secondary_persona**: Secondary classification
- **tertiary_persona**: Tertiary classification

---

### 5. vw_fact_cdr_geo_agg_hour_v2
**Type**: CDR Geographic Aggregation View (Call Detail Records with Location)
**Partition**: Daily (`par_day`) + Hourly (`par_hour`)

#### Purpose
Aggregated Call Detail Record (CDR) data with geographic information. Provides hourly-level location data for customers based on cell tower connections.

#### Key Features
- **Granularity**: Hourly CDR data with geographic precision
- **Partition Strategy**: par_day (YYYYMMDD) and par_hour (0-23 hour format)
- **Sample Date Range**: March 2024 onwards
- **Time Windows**: Earliest and latest connection timestamps within hour

#### Core Columns
- **msisdn** (hashed): Customer identifier
- **cellsite_sk**: Cell site identifier
- **cell_sector**: Cell sector code (e.g., "PCT1124T_S01")
- **cell_name**: Human-readable cell name (e.g., "PCT1124T_2NB01_S01")
- **earliest_start_datetime**: Earliest connection time in the hour
- **latest_end_datetime**: Latest disconnection time in the hour
- **total_duration**: Total connection duration in seconds
- **probability**: Confidence score for location estimate (0-1)
- **source**: Data source indicator (e.g., "cdr")
- **user_estimated_lat/long**: Estimated latitude/longitude of user location

#### Use Cases
- Geographic movement tracking
- Location-based customer segmentation
- Heat map generation for network optimization
- Travel pattern analysis
- Regional marketing campaigns
- Traffic pattern analysis by area

#### Example Query
```sql
SELECT 
    msisdn, 
    user_estimated_lat, 
    user_estimated_long, 
    cell_name,
    earliest_start_datetime,
    total_duration
FROM poc_mcp.vw_fact_cdr_geo_agg_hour_v2
WHERE par_day = '20240310'
AND par_hour = 17
```

---

### 6. vw_custom_detail_beha_cellsite_usage_hour_daily
**Type**: Custom Behavior View - Cellsite Usage (Hourly Daily Aggregation)
**Partition**: Daily (`par_day`) + Hourly (`par_hour`)

#### Purpose
Detailed behavioral data aggregated by cellsite usage on an hourly basis. Provides customer location and network usage patterns.

#### Key Features
- **Granularity**: Hourly cellsite usage data per customer
- **Partition Strategy**: par_day (YYYYMMDD) and par_hour (0-23 hour format)
- **Sample Date Range**: February 2023 onwards
- **Location Precision**: Latitude/longitude coordinates per cellsite

#### Core Columns
- **msisdn** (hashed): Customer identifier
- **cellsite_sk**: Cell site identifier/hash
- **latitude**: Cell site latitude coordinate
- **longitude**: Cell site longitude coordinate
- **par_day**: Partition day (YYYYMMDD format)
- **par_hour**: Partition hour (0-23 format)

#### Use Cases
- Hourly customer location tracking
- Cellsite capacity planning
- Customer mobility patterns
- Geographic clustering for network optimization
- Area-based customer behavior analysis
- Real-time location-based services

#### Data Characteristics
- Sparse/aggregated hourly data
- Only records with actual cellsite usage are included
- Hashed MSISDN for privacy protection
- Precise geographic coordinates for network-level analysis

#### Example Query
```sql
SELECT 
    msisdn, 
    latitude, 
    longitude,
    COUNT(*) as connection_count
FROM poc_mcp.vw_custom_detail_beha_cellsite_usage_hour_daily
WHERE par_day = '20230217'
AND par_hour BETWEEN 9 AND 11
GROUP BY msisdn, latitude, longitude
```

---

### 7. vw_fact_seq_agg_hour
**Type**: Network Flow/Session Aggregation (Hourly)
**Partition**: Daily (`par_day`) + Hourly (`par_hour`)

#### Purpose
Hourly aggregation of customer network sessions/flows. Tracks detailed network traffic sequences at hourly granularity including application usage, data consumption, and content categories.

#### Core Columns
- **subscriber_sk**: Subscriber identifier (hashed)
- **msisdn**: Phone number (hashed)
- **handsets_sk**: Device/handset identifier
- **cellsite_sk**: Cell tower identifier
- **imsi_prefix**: Mobile network code
- **server_hostname**: Destination server hostname
- **domainname**: Domain name accessed
- **service**: Service/application name (Facebook, TikTok, Instagram, etc.)
- **content_categories**: Content category (social, shopping, tools, maps_navigation, etc.)
- **flow_start_sec**: Session start timestamp
- **flow_end_sec**: Session end timestamp
- **duration**: Session duration in seconds
- **octet_total_cnt**: Total data transferred (bytes)
- **packet_total_cnt**: Total packets transmitted
- **incoming_octets**: Downloaded data (bytes)
- **outgoing_octets**: Uploaded data (bytes)
- **hit_cnt**: Number of hits/connections
- **session_hit_count**: Count of sessions in the period
- **source**: Data source indicator (seq for sequence data)

#### Key Features
- **Granularity**: Hourly-level network sessions
- **Partition Strategy**: par_day (YYYYMMDD) and par_hour (0-23 hour format)
- **Time Range**: April 2025 data available
- **Data Points**: Individual app sessions with traffic metrics

#### Use Cases
- Hourly application usage tracking
- Peak traffic analysis by service
- Data consumption patterns by app
- Network behavior analysis
- Content category preferences
- Service/app engagement metrics
- Real-time data usage monitoring

---

### 8. vw_fact_seq_agg_day
**Type**: Network Flow/Session Aggregation (Daily)
**Partition**: Daily (`par_day`)

#### Purpose
Daily aggregation of customer network sessions/flows. Provides day-level network traffic sequences for trend analysis and daily reporting by service/application.

#### Core Columns
Same as vw_fact_seq_agg_hour:
- **subscriber_sk, msisdn, handsets_sk, cellsite_sk**: Customer and device identifiers
- **service**: Application name (Facebook, TikTok, Instagram, Apple Maps, etc.)
- **content_categories**: Content type (social, shopping, maps_navigation, etc.)
- **flow_start_sec / flow_end_sec**: Session timing
- **duration**: Session length in seconds
- **octet_total_cnt, incoming_octets, outgoing_octets**: Data traffic metrics
- **hit_cnt, session_hit_count**: Frequency metrics

#### Key Features
- **Granularity**: Daily-level network sessions (aggregated from hourly/session data)
- **Partition Strategy**: par_day (YYYYMMDD format)
- **Time Range**: July 2025 data available
- **Data Aggregation**: Multiple sessions per customer/service per day

#### Example Data Points
- Facebook traffic: 112 KB, 289 packets, 38-second sessions
- TikTok traffic: 43 MB, 324 packets, 50-minute sessions
- Instagram traffic: 165 KB, 2,067 packets, 60-minute sessions
- Apple Maps: 343 bytes, 12-second navigation session
- Shopping apps (Vivo): 588 bytes, 1-second session

#### Use Cases
- Daily app usage trends
- Daily data consumption by service
- Service popularity tracking
- Daily network traffic reports
- Customer engagement by app
- Content consumption patterns
- Daily KPI dashboards

---

---

## Sequence/Network Flow Views Deep Dive

The three `vw_fact_seq_agg_*` views track **individual network sessions and flows** from your telecommunications network. These are NOT generic events or transactions, but actual **network traffic data** showing which apps customers use, how much data they consume, and for how long.

### What Data is Captured
Each row represents a customer's session with a specific application/service:
- **Which app**: TikTok, Facebook, Instagram, WhatsApp, Apple Maps, etc.
- **Content type**: Social media, shopping, tools, maps_navigation, standardprotocol
- **Traffic volumes**: Both download (incoming) and upload (outgoing) data in bytes and packets
- **Session duration**: Exact start/end times and total duration
- **Network context**: Which cellsite, device, and subscriber was involved
- **Access patterns**: Domain, server hostname, hit counts

### Time Aggregation Hierarchy
```
Individual Flow (seconds level)
         ↓
   Hour (multiple flows per hour)
         ↓
   Day (multiple flows per day)
         ↓
   Month (multiple flows per month)
```

### Example Scenarios

**Hour View** (April 2, 2025, Hour 1):
- Customer uses TikTok for ~50 minutes, consuming 43 MB
- Same customer briefly uses Facebook (10 seconds, 26 KB)
- Another customer navigates with Apple Maps (12 seconds, 343 bytes)

**Day View** (July 22, 2025):
- Customer session with Facebook: 38 seconds, 112 KB
- Customer session with Vivo shopping app: 1 second, 588 bytes
- Customer SSL connection: 1 second, 5.4 KB

**Month View** (October 2023):
- Customer's TikTok usage: 8 sessions totaling 882 MB over 71k seconds
- Facebook usage: Multiple sessions aggregated to 13 MB and 13k packets
- Cumulative session_hit_count = 4 (four distinct sessions)

---

### 9. vw_fact_seq_agg_month
**Type**: Network Flow/Session Aggregation (Monthly)
**Partition**: Monthly (`par_month`)

#### Purpose
Monthly aggregation of customer network sessions/flows. Provides month-level network traffic analysis for long-term trends and monthly business reporting.

#### Core Columns
Same as daily view:
- **subscriber_sk, msisdn, handsets_sk, cellsite_sk**: Customer and device identifiers
- **service**: Application name (TikTok, Facebook, Instagram, Apple, etc.)
- **content_categories**: Content type (social, shopping, tools, etc.)
- **flow_start_sec / flow_end_sec**: Session timing
- **duration**: Aggregated session duration
- **octet_total_cnt, incoming_octets, outgoing_octets**: Aggregated data traffic
- **hit_cnt, session_hit_count**: Aggregated frequency metrics

#### Key Features
- **Granularity**: Monthly-level network sessions (aggregated across all days in month)
- **Partition Strategy**: par_month (YYYYMM format)
- **Time Range**: October 2023 data available
- **Aggregation Level**: Sum of all sessions per customer/service/month

#### Example Data Points
- TikTok monthly: 882 MB, 1,175 packets, 8 sessions, 71k seconds duration
- Facebook monthly: 13 MB, 13k packets, 3 sessions in one month
- Multi-session tracking: session_hit_count = 4 (4 separate sessions aggregated)

#### Use Cases
- Monthly business reports
- Service popularity trends
- Seasonal content consumption analysis
- Year-over-year comparisons
- Long-term customer behavior patterns
- Monthly revenue impact analysis by service
- Quarterly/annual planning insights

---

## Data Characteristics

### Available Views Summary
- **vw_customer360_current**: Current snapshot (latest state)
- **vw_customer360**: Monthly partitioned customer view
- **vw_customer360_snapshot**: Daily partitioned historical snapshots
- **vw_fact_cdr_geo_agg_hour_v2**: Hourly CDR data with geographic coordinates
- **vw_custom_detail_beha_cellsite_usage_hour_daily**: Hourly cellsite usage behavior data
- **vw_fact_seq_agg_hour**: Hourly sequence/event aggregation
- **vw_fact_seq_agg_day**: Daily sequence/event aggregation
- **vw_fact_seq_agg_month**: Monthly sequence/event aggregation

### Data Quality
- **Coverage**: Comprehensive customer profiles with 300+ columns
- **Null Handling**: Many fields contain null values, indicating optional or sparse data
- **Consent Management**: All records include JSON consent_tuc field tracking user permissions

### Partition Strategy
- **vw_customer360_current**: No partition (current data only)
- **vw_customer360**: Monthly partitions (par_month: YYYYMM format)
  - Example: 202509 (September 2025)
  - Current data available for recent months

- **vw_customer360_snapshot**: Daily partitions (par_day: YYYYMMDD format)
  - Example: 20250601 (June 1, 2025)
  - Historical snapshots available

- **vw_fact_cdr_geo_agg_hour_v2**: Dual partitions (par_day + par_hour)
  - par_day: YYYYMMDD format (example: 20240310)
  - par_hour: 0-23 hour format
  - Data from March 2024 onwards

- **vw_custom_detail_beha_cellsite_usage_hour_daily**: Dual partitions (par_day + par_hour)
  - par_day: YYYYMMDD format (example: 20230217)
  - par_hour: 0-23 hour format
  - Data from February 2023 onwards

- **vw_fact_seq_agg_hour**: Dual partitions (par_day + par_hour)
  - par_day: YYYYMMDD format
  - par_hour: 0-23 hour format
  - Hourly-level event sequences

- **vw_fact_seq_agg_day**: Single partition (par_day)
  - par_day: YYYYMMDD format
  - Daily-level event sequences

- **vw_fact_seq_agg_month**: Single partition (par_month)
  - par_month: YYYYMM format
  - Monthly-level event sequences

### Data Types
- **Strings**: Most fields (categories, IDs, locations)
- **Integers**: Counts, numeric codes, age, frequency
- **Timestamps**: created_datetime, updated_datetime
- **JSON**: consent_tuc (complex consent tracking)
- **Binary/Boolean**: Flags like activated_flag, is_tdg_customer, is_blocklist

---

## Common Query Patterns

### 1. Customer Segmentation
```
SELECT msisdn, uid, ban_num, 
       demo_age_group_v1_age_grp_cat,
       demo_gender_v1_gender_bin,
       demo_lifestage_v2_lifestage_grp_cat
FROM poc_mcp.vw_customer360
WHERE par_month = '202509'
```

### 2. Interest-Based Targeting
```
SELECT msisdn, 
       inte_standard_label_all_cat,
       inte_entertainment_*
FROM poc_mcp.vw_customer360
WHERE par_month = '202509'
AND inte_standard_label_all_cat IS NOT NULL
```

### 3. Geographic Analysis
```
SELECT geog_resident_location_province_en_cat,
       COUNT(DISTINCT msisdn) as customer_count
FROM poc_mcp.vw_customer360
WHERE par_month = '202509'
GROUP BY geog_resident_location_province_en_cat
```

### 4. Behavioral Segmentation
```
SELECT msisdn,
       beha_mobile_data_usage_monthly_data_usage_grp_cat,
       beha_mobile_calling_usage_monthly_v1_voice_inc_duration_grp
FROM poc_mcp.vw_customer360
WHERE par_month = '202509'
```

### 5. Historical Trend Analysis
```
SELECT par_day, msisdn, 
       demo_lifestage_v2_lifestage_grp_cat,
       primary_persona
FROM poc_mcp.vw_customer360_snapshot
WHERE par_day BETWEEN '20250501' AND '20250630'
```

---

## MCP Server Capabilities

The Databricks MCP server provides the following functions:

### Available Operations
1. **list_catalogs()** - Browse available catalogs
2. **list_schemas(catalog)** - List schemas within a catalog
3. **list_tables(catalog, schema)** - List tables/views in a schema
4. **partition_info(catalog, schema, table)** - Get partition information
5. **table_metadata(catalog, schema, table)** - Get table structure/columns
6. **sample_data(catalog, schema, table, limit)** - Preview data
7. **preview_query(sql, limit)** - Test SQL queries before running
8. **run_query(sql, limit)** - Execute analytical queries
9. **health_check()** - Verify connection status

---

## Integration with Databricks MCP POC Project

### Current Setup
- Located in `/workspaces/poc-databricks-mcp`
- Configuration files: `config.yml`, `config.example.yml`
- Main source: `src/databricks_mcp/`

### Key Components
- **client.py**: Databricks API client
- **server.py**: MCP server implementation
- **config.py**: Configuration management
- **auth.py**: Authentication handling
- **guardrails.py**: Query safety checks
- **errors.py**: Error handling

### Recommended Next Steps
1. **Create analysis scripts** using the available data
2. **Build customer segmentation models** using demographic + behavioral data
3. **Develop targeting campaigns** based on interest profiles
4. **Analyze geographic patterns** for localized marketing
5. **Track customer journey** using snapshot data over time
6. **Monitor consent** for GDPR/privacy compliance

---

## Data Governance

### Privacy & Consent
- All customer identifiers are hashed (msisdn, uid, cert_id)
- Explicit consent tracking via consent_tuc JSON field
- Blocklist indicators for non-consenting customers (is_blocklist, demo_blocklist_blocklist_bin)

### Data Categories
- **PII**: hashed identifiers, location data
- **Behavioral**: usage patterns, content preferences
- **Financial**: payment profiles, spending categories
- **Demographic**: age, gender, life stage

---

## Performance Notes

### Query Considerations
- Monthly partition support enables efficient filtering
- Large number of columns (300+) may impact query performance
- NULL values common in interest/behavioral columns
- Consider selective column projection in queries

### Recommended Optimizations
1. Filter by partition (par_month or par_day) early
2. Project only required columns
3. Use aggregations to reduce result sizes
4. Cache frequently accessed segments
5. Consider incremental analysis over full scans

---

## Additional Resources

- **Databricks Documentation**: https://docs.databricks.com
- **SQL Functions**: Available in Unity Catalog
- **Related Files**:
  - Project README: `./README.md`
  - Requirements: `./REQUIREMENTS.md`
  - Project Config: `./pyproject.toml`
