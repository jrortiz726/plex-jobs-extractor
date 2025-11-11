# Comprehensive Data Mapping and Analytics Guide for Plex-CDF Extractors

## Executive Summary

This document provides a comprehensive guide for leveraging the Plex-CDF standalone extractors for advanced analytics, predictive modeling, and strategic decision-making. The extractors have been designed with rich metadata structures that enable Natural Language Queries (NQL), industry-standard KPI calculations, and machine learning applications.

## Table of Contents

1. [NQL Query Optimization](#1-nql-query-optimization)
2. [Industry Metrics and KPIs](#2-industry-metrics-and-kpis)
3. [Chart and Graph Recommendations](#3-chart-and-graph-recommendations)
4. [Predictive Analytics Opportunities](#4-predictive-analytics-opportunities)
5. [Data Quality and Completeness](#5-data-quality-and-completeness)
6. [Implementation Roadmap](#6-implementation-roadmap)

---

## 1. NQL Query Optimization

### 1.1 Metadata Structure Overview

Each extractor implements rich metadata tagging specifically designed for NQL queries:

#### **Jobs Extractor Metadata**
```json
{
  "pcn": "customer_id",
  "facility": "facility_name",
  "source": "plex_jobs",
  "job_id": "unique_identifier",
  "job_number": "human_readable_number",
  "status": "Scheduled|In Progress|Completed",
  "part_number": "part_identifier",
  "part_name": "descriptive_name",
  "workcenter": "workcenter_code",
  "quantity": "formatted_quantity",
  "priority": "priority_level",
  "customer": "customer_name",
  "order_number": "sales_order"
}
```

#### **Production Extractor Metadata**
```json
{
  "workcenter_id": "identifier",
  "workcenter_type": "machining|assembly|inspection",
  "status": "running|idle|down|maintenance",
  "oee": "overall_equipment_effectiveness",
  "availability": "percentage",
  "performance": "percentage",
  "quality": "percentage",
  "production_rate": "units_per_hour",
  "first_time_yield": "percentage",
  "defect_rate_ppm": "parts_per_million",
  "shift": "first|second|third",
  "is_bottleneck": "boolean",
  "needs_attention": "boolean"
}
```

### 1.2 Sample NQL Queries

#### **Operational Queries**

```sql
-- Find bottleneck workcenters affecting production
"Show me all workcenters where is_bottleneck is true and OEE is less than 60%"

-- Identify quality issues by shift
"What is the average first_time_yield by shift for the last 7 days?"

-- Track job completion status
"How many jobs with status 'In Progress' have priority 'high'?"

-- Monitor real-time production
"Show current production rate for all running workcenters"
```

#### **Strategic Queries**

```sql
-- Capacity planning
"What is the average utilization by workcenter_type over the last month?"

-- Quality trends
"Show me the trend of defect_rate_ppm for part_type 'critical' over the last quarter"

-- Supply chain optimization
"Which suppliers have quality_rating below 80 and delivery_rating below 90?"

-- Inventory optimization
"List all parts with stockout_risk > 0.2 and classification 'AA'"
```

#### **Predictive Queries**

```sql
-- Maintenance prediction
"Which workcenters have declining OEE trends and high downtime in the last 30 days?"

-- Demand forecasting
"Show parts with demand_variability > 0.25 and forecast_accuracy < 80%"

-- Quality prediction
"Identify production entries where process parameters deviate from control limits"
```

### 1.3 Query Optimization Best Practices

1. **Use Indexed Fields**: Priority fields for indexing:
   - `workcenter_id`, `part_id`, `job_id`
   - `timestamp`, `shift`, `status`
   - `classification`, `criticality`

2. **Leverage Hierarchies**:
   - Facility → Workcenter → Machine
   - Part → BOM → Component
   - Location → Container → Item

3. **Time-based Aggregations**:
   - Use pre-calculated hourly/daily/weekly rollups
   - Leverage time series data for trending

---

## 2. Industry Metrics and KPIs

### 2.1 KPI Mapping to Industry Standards

#### **ISO 22400 - Manufacturing Operations Management KPIs**

| KPI | Formula | Extractor Source | Industry Target |
|-----|---------|------------------|-----------------|
| **OEE** | Availability × Performance × Quality | Production | 85% (World Class) |
| **TEEP** | OEE × Utilization | Production | 65% |
| **First Time Yield** | Good Units / Total Units | Quality | 99% |
| **Scrap Rate** | Scrap Qty / Total Qty | Production | < 1% |
| **Setup Time** | Total Setup / # of Setups | Production | < 10 min |

#### **MESA Model (Manufacturing Enterprise Solutions Association)**

| Metric | Calculation | Data Source | Benchmark |
|--------|-------------|-------------|-----------|
| **Customer OTD** | On-time Orders / Total Orders | Jobs | > 98% |
| **Schedule Attainment** | Actual Output / Scheduled Output | Production | > 95% |
| **Capacity Utilization** | Actual Hours / Available Hours | Production | > 80% |
| **Total Cycle Time** | Production Time + Queue Time | Jobs + Production | Varies |
| **Yield** | Good Output / Total Input | Quality | > 95% |

#### **ISA-95 Level 3 Metrics**

```python
# Production Performance Metrics
{
    "production_performance": {
        "actual_vs_planned": "actual_quantity / planned_quantity",
        "efficiency": "standard_time / actual_time",
        "productivity": "output / (labor_hours + machine_hours)",
        "unit_cost": "total_cost / good_units"
    },

    "quality_performance": {
        "first_pass_yield": "first_time_good / total_produced",
        "rework_rate": "rework_quantity / total_quantity",
        "scrap_rate": "scrap_quantity / total_quantity",
        "customer_returns": "returned_units / shipped_units"
    },

    "maintenance_performance": {
        "mtbf": "operating_time / number_of_failures",
        "mttr": "total_repair_time / number_of_repairs",
        "availability": "uptime / scheduled_time"
    }
}
```

### 2.2 Cross-Functional Executive KPIs

#### **C-Suite Dashboard Metrics**

| Executive | Primary KPIs | Secondary KPIs | Data Sources |
|-----------|--------------|----------------|--------------|
| **CEO** | Revenue per Employee<br>EBITDA Margin<br>Customer Satisfaction | Market Share<br>Innovation Index<br>ESG Score | All Extractors |
| **CFO** | Working Capital<br>Cash Conversion Cycle<br>ROCE | Inventory Turns<br>DSO/DPO<br>CapEx Efficiency | Master + Inventory |
| **COO** | OEE<br>On-Time Delivery<br>Cost per Unit | Capacity Utilization<br>Lead Time<br>Schedule Adherence | Production + Jobs |
| **CQO** | First Time Yield<br>Cost of Quality<br>Customer PPM | Cpk/Ppk<br>Supplier Quality<br>NCR Closure Rate | Quality |
| **CSCO** | Perfect Order Rate<br>Supply Chain Cycle Time<br>Inventory Accuracy | Supplier Performance<br>Forecast Accuracy<br>Fill Rate | Inventory + Master |

### 2.3 KPI Calculation Formulas

```python
# Financial KPIs
def calculate_financial_kpis(data):
    return {
        "revenue_per_employee": data.revenue / data.employee_count,
        "ebitda_margin": (data.ebitda / data.revenue) * 100,
        "working_capital_turns": data.revenue / data.working_capital,
        "roce": (data.ebit / data.capital_employed) * 100,
        "inventory_turns": data.cogs / data.average_inventory,
        "cash_conversion_cycle": data.dso + data.dio - data.dpo
    }

# Operational KPIs
def calculate_operational_kpis(production_data):
    return {
        "oee": production_data.availability * production_data.performance * production_data.quality / 10000,
        "productivity": production_data.output / production_data.total_hours,
        "schedule_attainment": (production_data.actual_output / production_data.scheduled_output) * 100,
        "capacity_utilization": (production_data.used_capacity / production_data.available_capacity) * 100,
        "changeover_time": production_data.total_changeover / production_data.changeover_count
    }

# Quality KPIs
def calculate_quality_kpis(quality_data):
    return {
        "fty": (quality_data.first_time_good / quality_data.total_inspected) * 100,
        "dpmo": (quality_data.defects / quality_data.opportunities) * 1000000,
        "cost_of_quality": quality_data.prevention_cost + quality_data.appraisal_cost + quality_data.failure_cost,
        "supplier_ppm": (quality_data.supplier_defects / quality_data.supplier_parts) * 1000000,
        "customer_ppm": (quality_data.customer_returns / quality_data.shipped_units) * 1000000
    }
```

---

## 3. Chart and Graph Recommendations

### 3.1 Visualization Matrix by Data Domain

#### **Production Domain**

| Metric | Chart Type | Update Frequency | User Persona |
|--------|------------|------------------|--------------|
| **Real-time OEE** | Gauge + Sparkline | 1 minute | Operations Manager |
| **OEE Trend** | Multi-line Chart | Hourly | Plant Manager |
| **Six Big Losses** | Waterfall Chart | Daily | Continuous Improvement |
| **Workcenter Status** | Heat Map | Real-time | Supervisor |
| **Production vs Plan** | Bullet Chart | Shift | Production Planner |
| **Cycle Time Distribution** | Histogram + Control Limits | Hourly | Quality Engineer |

#### **Quality Domain**

| Metric | Chart Type | Update Frequency | User Persona |
|--------|------------|------------------|--------------|
| **SPC Control Charts** | X-bar/R Chart | Per Batch | Quality Engineer |
| **Defect Pareto** | Pareto Chart | Daily | Quality Manager |
| **First Time Yield** | Line + Target Line | Shift | Operations Manager |
| **NCR Aging** | Stacked Bar | Weekly | Quality Director |
| **Supplier Quality** | Scatter Plot Matrix | Monthly | Supplier Quality |
| **Process Capability** | Cpk Distribution | Per Run | Process Engineer |

#### **Inventory Domain**

| Metric | Chart Type | Update Frequency | User Persona |
|--------|------------|------------------|--------------|
| **ABC-XYZ Matrix** | Bubble Chart | Weekly | Inventory Manager |
| **Stock Levels** | Area Chart + Min/Max | Daily | Warehouse Manager |
| **Inventory Turns** | KPI Card + Trend | Monthly | CFO |
| **Stockout Risk** | Risk Matrix | Daily | Supply Chain |
| **Aging Analysis** | Stacked Area | Weekly | Finance |
| **Location Utilization** | Tree Map | Daily | Warehouse Manager |

### 3.2 Executive Dashboard Layouts

#### **Plant Manager Dashboard**
```
┌─────────────────────────────────────────────────────────┐
│                    PLANT OVERVIEW                        │
├──────────┬──────────┬──────────┬──────────┬────────────┤
│   OEE    │   FTY    │   OTD    │  Output  │   Safety   │
│  85.2%   │  98.7%   │  97.3%   │  10,234  │  0 Days    │
│   ▲2.1   │   ▼0.3   │   ▲1.2   │   ▲234   │            │
├──────────┴──────────┴──────────┴──────────┴────────────┤
│              OEE TREND (7 DAYS)           │ LOSSES      │
│  [Line Chart with Target Line]            │ [Waterfall] │
├───────────────────────────────────────────┼─────────────┤
│     WORKCENTER PERFORMANCE MATRIX         │  ALERTS     │
│         [Heat Map by OEE]                 │  • WC-01 ↓  │
│                                           │  • NCR-234  │
└───────────────────────────────────────────┴─────────────┘
```

#### **Supply Chain Dashboard**
```
┌─────────────────────────────────────────────────────────┐
│                 SUPPLY CHAIN METRICS                     │
├──────────┬──────────┬──────────┬──────────┬────────────┤
│ Inventory│  Turns   │ Fill Rate│ Forecast │  Perfect   │
│  $2.3M   │   12.4   │  98.2%   │  82.3%   │   94.5%    │
├──────────┴──────────┴──────────┴──────────┴────────────┤
│        INVENTORY BY CLASSIFICATION        │  STOCKOUTS  │
│      [ABC-XYZ Bubble Chart]              │  [Risk Map] │
├───────────────────────────────────────────┼─────────────┤
│          DEMAND vs SUPPLY                 │  SUPPLIERS  │
│       [Dual Axis Line Chart]             │  [Scorecard]│
└───────────────────────────────────────────┴─────────────┘
```

### 3.3 Real-time vs Historical Visualization

| Visualization Need | Real-time (< 5 min) | Near Real-time (5-60 min) | Historical (> 1 hour) |
|-------------------|---------------------|---------------------------|----------------------|
| **Production Status** | Status Board, Andon | OEE Gauges, Throughput | Trend Analysis, Reports |
| **Quality Alerts** | SPC Violations | Control Charts | Capability Studies |
| **Inventory Levels** | Critical Items | Reorder Alerts | ABC Analysis |
| **Machine Status** | Up/Down Indicators | Utilization Meters | MTBF/MTTR Analysis |

---

## 4. Predictive Analytics Opportunities

### 4.1 Machine Learning Use Cases by Domain

#### **Production Analytics**

| Use Case | ML Model | Features | Expected Benefit |
|----------|----------|----------|------------------|
| **Predictive Maintenance** | Random Forest/LSTM | Vibration, Temperature, OEE Trend, Cycle Count | 30% reduction in unplanned downtime |
| **Optimal Scheduling** | Genetic Algorithm | Job Queue, Setup Times, Due Dates | 15% improvement in OTD |
| **Bottleneck Prediction** | Gradient Boosting | WIP Levels, Cycle Times, Queue Lengths | 20% increase in throughput |
| **Energy Optimization** | Neural Network | Production Schedule, Machine States, Shift Patterns | 10% energy cost reduction |

#### **Quality Analytics**

| Use Case | ML Model | Features | Expected Benefit |
|----------|----------|----------|------------------|
| **Defect Prediction** | XGBoost | Process Parameters, Material Properties, Operator | 40% reduction in defects |
| **SPC Anomaly Detection** | Isolation Forest | Measurement Data, Control Limits, Patterns | 95% detection accuracy |
| **Root Cause Analysis** | Decision Trees | Defect Codes, Process Data, Environmental | 50% faster problem resolution |
| **Supplier Risk Scoring** | Logistic Regression | Quality History, Delivery Performance, Audits | 25% reduction in supplier defects |

#### **Inventory Analytics**

| Use Case | ML Model | Features | Expected Benefit |
|----------|----------|----------|------------------|
| **Demand Forecasting** | ARIMA/Prophet | Historical Demand, Seasonality, Events | 20% improvement in forecast accuracy |
| **Optimal Stock Levels** | Reinforcement Learning | Demand Variability, Lead Times, Service Levels | 15% reduction in inventory cost |
| **Obsolescence Prediction** | Survival Analysis | Age, Movement, Product Lifecycle | 30% reduction in obsolete inventory |
| **Dynamic Reorder Points** | Bayesian Optimization | Demand Patterns, Lead Time Variability | 10% reduction in stockouts |

### 4.2 Feature Engineering Recommendations

```python
# Production Features
production_features = {
    "temporal": [
        "hour_of_day", "day_of_week", "shift_id", "is_weekend",
        "time_since_maintenance", "time_since_changeover"
    ],
    "rolling_statistics": [
        "oee_rolling_mean_24h", "oee_rolling_std_7d",
        "defect_rate_ewma", "cycle_time_zscore"
    ],
    "lagged_variables": [
        "oee_lag_1h", "oee_lag_1d", "oee_lag_1w",
        "production_lag_1shift", "quality_lag_1batch"
    ],
    "interaction_features": [
        "operator_x_shift", "part_x_workcenter",
        "material_x_temperature", "speed_x_feed_rate"
    ],
    "engineered_ratios": [
        "actual_vs_ideal_cycle", "setup_to_run_ratio",
        "good_to_total_ratio", "value_added_time_ratio"
    ]
}

# Quality Features
quality_features = {
    "statistical_process": [
        "cpk", "ppk", "process_sigma", "control_limit_violations",
        "western_electric_rules", "trend_detection"
    ],
    "defect_patterns": [
        "defect_clustering", "defect_frequency", "defect_severity_score",
        "time_between_defects", "defect_correlation_matrix"
    ],
    "supplier_metrics": [
        "supplier_quality_score", "supplier_delivery_score",
        "supplier_risk_index", "supplier_improvement_trend"
    ]
}

# Inventory Features
inventory_features = {
    "demand_characteristics": [
        "coefficient_of_variation", "demand_trend", "seasonality_strength",
        "intermittent_demand_indicator", "demand_forecast_error"
    ],
    "inventory_metrics": [
        "days_on_hand", "stockout_frequency", "excess_stock_days",
        "inventory_velocity", "fill_rate_rolling"
    ],
    "supply_chain": [
        "lead_time_variability", "supplier_reliability",
        "transit_time_variance", "order_frequency"
    ]
}
```

### 4.3 ML Model Implementation Pipeline

```python
class PredictiveAnalyticsPipeline:
    """Template for implementing predictive analytics"""

    def __init__(self, domain: str):
        self.domain = domain
        self.model = None
        self.feature_pipeline = None

    def prepare_features(self, raw_data):
        """Feature engineering pipeline"""
        features = pd.DataFrame()

        # Time-based features
        features['hour'] = raw_data['timestamp'].dt.hour
        features['dayofweek'] = raw_data['timestamp'].dt.dayofweek
        features['month'] = raw_data['timestamp'].dt.month

        # Rolling statistics
        for window in [24, 168, 720]:  # 1 day, 1 week, 1 month
            features[f'rolling_mean_{window}h'] = raw_data['value'].rolling(window).mean()
            features[f'rolling_std_{window}h'] = raw_data['value'].rolling(window).std()

        # Lag features
        for lag in [1, 24, 168]:
            features[f'lag_{lag}h'] = raw_data['value'].shift(lag)

        return features

    def train_model(self, features, target):
        """Train predictive model"""
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.model_selection import TimeSeriesSplit

        # Time series cross-validation
        tscv = TimeSeriesSplit(n_splits=5)

        # Model training
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=20
        )

        # Feature importance analysis
        self.model.fit(features, target)
        self.feature_importance = pd.DataFrame({
            'feature': features.columns,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)

        return self.model

    def predict(self, features):
        """Generate predictions with confidence intervals"""
        predictions = self.model.predict(features)

        # Calculate prediction intervals (simplified)
        std_dev = np.std(self.model.predict(features) - features)
        lower_bound = predictions - 1.96 * std_dev
        upper_bound = predictions + 1.96 * std_dev

        return {
            'prediction': predictions,
            'lower_95': lower_bound,
            'upper_95': upper_bound
        }
```

---

## 5. Data Quality and Completeness

### 5.1 Data Validation Strategy

#### **Validation Rules by Domain**

```python
# Production Data Validation
production_validation = {
    "completeness_checks": [
        "workcenter_id NOT NULL",
        "timestamp NOT NULL",
        "oee BETWEEN 0 AND 100",
        "cycle_time > 0"
    ],
    "consistency_checks": [
        "actual_quantity <= planned_quantity * 1.2",  # Max 20% overproduction
        "good_quantity <= actual_quantity",
        "oee = availability * performance * quality / 10000"
    ],
    "business_rules": [
        "downtime + runtime <= scheduled_time",
        "scrap_quantity + good_quantity = actual_quantity",
        "IF status = 'RUNNING' THEN current_job_id NOT NULL"
    ]
}

# Quality Data Validation
quality_validation = {
    "statistical_checks": [
        "measurements WITHIN 6-sigma of mean",
        "cpk BETWEEN -10 AND 10",
        "defect_rate >= 0"
    ],
    "spc_rules": [
        "NO more than 1 point beyond 3-sigma in 25",
        "NO 7 consecutive points on one side of center",
        "NO 7 consecutive points trending"
    ]
}

# Inventory Data Validation
inventory_validation = {
    "balance_checks": [
        "on_hand = available + allocated",
        "ending_balance = beginning_balance + receipts - issues",
        "total_value = quantity * unit_cost"
    ],
    "threshold_checks": [
        "safety_stock >= 0",
        "reorder_point >= safety_stock",
        "max_stock >= reorder_point + reorder_quantity"
    ]
}
```

### 5.2 Data Quality Monitoring

#### **Quality Metrics Dashboard**

| Metric | Calculation | Target | Alert Threshold |
|--------|-------------|--------|-----------------|
| **Completeness** | Non-null fields / Total fields | > 95% | < 90% |
| **Accuracy** | Validated records / Total records | > 98% | < 95% |
| **Timeliness** | On-time extractions / Total extractions | > 99% | < 95% |
| **Consistency** | Passed rules / Total rules | > 99% | < 97% |
| **Uniqueness** | Unique records / Total records | = 100% | < 100% |

### 5.3 Data Quality Issues and Remediation

| Issue Type | Detection Method | Remediation | Prevention |
|------------|------------------|-------------|------------|
| **Missing Data** | NULL checks, Gap analysis | Interpolation, Default values | Required field validation |
| **Duplicates** | Hash comparison, Key checks | Deduplication, Last value | Unique constraints |
| **Outliers** | Statistical limits, IQR | Investigation, Correction | Range validation |
| **Inconsistency** | Cross-reference checks | Data reconciliation | Referential integrity |
| **Latency** | Timestamp analysis | Backfill, Replay | SLA monitoring |

### 5.4 Monitoring and Alerting Framework

```python
class DataQualityMonitor:
    """Real-time data quality monitoring"""

    def __init__(self):
        self.alerts = []
        self.metrics = {}

    def check_completeness(self, df, required_fields):
        """Check for missing data"""
        completeness = {}
        for field in required_fields:
            if field in df.columns:
                completeness[field] = df[field].notna().mean()
                if completeness[field] < 0.9:
                    self.alerts.append({
                        'type': 'completeness',
                        'field': field,
                        'value': completeness[field],
                        'severity': 'high' if completeness[field] < 0.8 else 'medium'
                    })
        return completeness

    def check_consistency(self, df, rules):
        """Check business rule consistency"""
        consistency = {}
        for rule_name, rule_func in rules.items():
            consistency[rule_name] = rule_func(df).mean()
            if consistency[rule_name] < 0.95:
                self.alerts.append({
                    'type': 'consistency',
                    'rule': rule_name,
                    'value': consistency[rule_name],
                    'severity': 'high'
                })
        return consistency

    def check_anomalies(self, df, columns, method='zscore'):
        """Detect anomalies in data"""
        anomalies = {}
        for col in columns:
            if method == 'zscore':
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                anomalies[col] = (z_scores > 3).sum()
            elif method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                anomalies[col] = ((df[col] < Q1 - 1.5*IQR) | (df[col] > Q3 + 1.5*IQR)).sum()

            if anomalies[col] > df.shape[0] * 0.01:  # More than 1% anomalies
                self.alerts.append({
                    'type': 'anomaly',
                    'column': col,
                    'count': anomalies[col],
                    'severity': 'medium'
                })
        return anomalies

    def generate_quality_report(self):
        """Generate comprehensive quality report"""
        return {
            'timestamp': datetime.now(timezone.utc),
            'metrics': self.metrics,
            'alerts': self.alerts,
            'quality_score': self._calculate_quality_score(),
            'recommendations': self._generate_recommendations()
        }

    def _calculate_quality_score(self):
        """Calculate overall data quality score"""
        scores = []
        if 'completeness' in self.metrics:
            scores.append(np.mean(list(self.metrics['completeness'].values())))
        if 'consistency' in self.metrics:
            scores.append(np.mean(list(self.metrics['consistency'].values())))
        if 'accuracy' in self.metrics:
            scores.append(self.metrics['accuracy'])

        return np.mean(scores) * 100 if scores else 0

    def _generate_recommendations(self):
        """Generate recommendations based on issues found"""
        recommendations = []

        high_severity_alerts = [a for a in self.alerts if a['severity'] == 'high']
        if high_severity_alerts:
            recommendations.append("Immediate attention required for high-severity data quality issues")

        completeness_issues = [a for a in self.alerts if a['type'] == 'completeness']
        if completeness_issues:
            recommendations.append("Review data collection process for missing fields")

        anomaly_issues = [a for a in self.alerts if a['type'] == 'anomaly']
        if anomaly_issues:
            recommendations.append("Investigate anomalous values for potential data collection errors")

        return recommendations
```

---

## 6. Implementation Roadmap

### 6.1 Phase 1: Foundation (Weeks 1-4)

#### **Objectives**
- Deploy standalone extractors
- Establish data quality baselines
- Implement core KPIs

#### **Deliverables**
1. **Data Infrastructure**
   - All 5 extractors deployed and running
   - Data quality monitoring dashboard
   - Historical data backfill completed

2. **Core KPIs**
   - OEE calculation and tracking
   - First Time Yield monitoring
   - On-Time Delivery metrics
   - Inventory accuracy

3. **Basic Visualizations**
   - Real-time production status board
   - Daily OEE trends
   - Quality control charts
   - Inventory levels dashboard

### 6.2 Phase 2: Analytics (Weeks 5-8)

#### **Objectives**
- Enable NQL queries
- Implement advanced KPIs
- Deploy predictive models

#### **Deliverables**
1. **NQL Implementation**
   - Metadata tagging completed
   - Query templates library
   - User training materials

2. **Advanced Analytics**
   - Balanced scorecard implementation
   - Industry benchmarking
   - Root cause analysis tools
   - ABC-XYZ inventory classification

3. **Predictive Models**
   - Demand forecasting model
   - Predictive maintenance pilot
   - Quality prediction model

### 6.3 Phase 3: Optimization (Weeks 9-12)

#### **Objectives**
- Optimize system performance
- Implement ML at scale
- Drive business value

#### **Deliverables**
1. **System Optimization**
   - Query performance tuning
   - Data pipeline optimization
   - Alert automation

2. **ML Production**
   - Model deployment pipeline
   - A/B testing framework
   - Model monitoring dashboard

3. **Business Impact**
   - Executive dashboards
   - Automated reporting
   - Decision support system

### 6.4 Success Metrics

| Phase | Metric | Target | Measurement |
|-------|--------|--------|-------------|
| **Phase 1** | Data Quality Score | > 95% | Automated monitoring |
| **Phase 1** | KPI Coverage | 100% core KPIs | Dashboard completion |
| **Phase 2** | NQL Adoption | > 50 queries/day | Query logs |
| **Phase 2** | Prediction Accuracy | > 85% | Model validation |
| **Phase 3** | Cost Savings | > $500K/year | Financial analysis |
| **Phase 3** | OEE Improvement | > 5% | Year-over-year |

### 6.5 Resource Requirements

#### **Technical Resources**
- Data Engineers: 2 FTE
- Data Scientists: 1 FTE
- BI Developers: 2 FTE
- DevOps Engineer: 0.5 FTE

#### **Infrastructure**
- Cognite Data Fusion licenses
- Compute resources for ML models
- Visualization platform (Grafana/PowerBI)
- Development and staging environments

#### **Training and Support**
- NQL query training for analysts
- Dashboard training for operators
- ML model interpretation training
- Ongoing support and maintenance

---

## Appendix A: Sample NQL Query Library

### Production Queries
```sql
-- Top 10 bottleneck analysis
SELECT workcenter_id, workcenter_name, AVG(oee) as avg_oee,
       COUNT(CASE WHEN is_bottleneck = true) as bottleneck_count
FROM production_data
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY workcenter_id, workcenter_name
ORDER BY avg_oee ASC
LIMIT 10

-- Shift performance comparison
SELECT shift,
       AVG(oee) as avg_oee,
       AVG(first_time_yield) as avg_fty,
       SUM(good_quantity) as total_good_units
FROM production_data
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY shift
ORDER BY avg_oee DESC

-- Downtime pattern analysis
SELECT
    EXTRACT(HOUR FROM start_time) as hour_of_day,
    category,
    AVG(duration_minutes) as avg_duration,
    COUNT(*) as frequency
FROM downtime_events
WHERE timestamp > NOW() - INTERVAL '90 days'
GROUP BY hour_of_day, category
ORDER BY frequency DESC
```

### Quality Queries
```sql
-- Supplier quality ranking
SELECT
    supplier_id,
    supplier_name,
    AVG(quality_rating) as avg_quality,
    SUM(defects) / SUM(delivered_quantity) * 1000000 as ppm
FROM supplier_quality_data
WHERE timestamp > NOW() - INTERVAL '6 months'
GROUP BY supplier_id, supplier_name
HAVING COUNT(*) > 10
ORDER BY ppm ASC

-- Process capability by part
SELECT
    part_number,
    characteristic,
    AVG(cpk) as avg_cpk,
    MIN(cpk) as min_cpk,
    COUNT(CASE WHEN cpk < 1.33) as below_target_count
FROM spc_data
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY part_number, characteristic
HAVING COUNT(*) > 30
ORDER BY avg_cpk ASC
```

### Inventory Queries
```sql
-- High-risk inventory items
SELECT
    part_number,
    location_id,
    quantity_on_hand,
    days_on_hand,
    stockout_risk,
    obsolescence_risk,
    total_value
FROM inventory_items
WHERE (stockout_risk > 0.2 OR obsolescence_risk > 0.3)
  AND classification IN ('AA', 'AB', 'BA')
ORDER BY total_value DESC

-- Inventory optimization opportunities
SELECT
    classification,
    COUNT(*) as item_count,
    SUM(total_value) as total_value,
    AVG(turnover_ratio) as avg_turnover,
    AVG(days_on_hand) as avg_days_on_hand
FROM inventory_items
GROUP BY classification
ORDER BY total_value DESC
```

## Appendix B: KPI Definition Templates

### Standard KPI Template
```yaml
kpi:
  id: "unique_kpi_id"
  name: "KPI Display Name"
  category: "operational|quality|financial|customer"
  description: "Detailed description of what this KPI measures"

  calculation:
    formula: "mathematical_formula"
    numerator: "data_field_1"
    denominator: "data_field_2"
    unit: "%, count, currency, time"
    aggregation: "sum|average|last|max|min"

  targets:
    minimum: 0
    target: 85
    stretch: 95
    world_class: 99

  visualization:
    primary_chart: "gauge|line|bar"
    secondary_chart: "sparkline|bullet"
    color_coding:
      red: "< 70"
      yellow: "70-85"
      green: "> 85"

  data_source:
    extractor: "production|quality|inventory"
    refresh_rate: "real-time|1min|5min|hourly|daily"
    retention: "7d|30d|90d|1y"

  alerts:
    critical:
      condition: "value < 60"
      recipients: ["operations_manager", "plant_manager"]
      frequency: "immediate"
    warning:
      condition: "value < 75"
      recipients: ["supervisor"]
      frequency: "hourly"
```

## Appendix C: ML Model Cards

### Predictive Maintenance Model
```yaml
model:
  name: "Workcenter Failure Prediction"
  version: "1.0.0"
  type: "classification"
  algorithm: "Random Forest"

  features:
    - oee_rolling_mean_7d
    - vibration_amplitude
    - temperature_delta
    - cycles_since_maintenance
    - age_days

  performance:
    accuracy: 0.92
    precision: 0.89
    recall: 0.94
    f1_score: 0.91

  training:
    dataset_size: 100000
    training_period: "2023-01 to 2024-06"
    validation_method: "time_series_cv"

  deployment:
    endpoint: "/api/v1/predict/maintenance"
    latency_p99: "100ms"
    throughput: "1000 req/s"

  monitoring:
    drift_detection: "enabled"
    retraining_trigger: "monthly|drift>0.1"
    performance_logging: "all_predictions"
```

---

## Conclusion

The Plex-CDF standalone extractors provide a robust foundation for advanced manufacturing analytics. The rich metadata structure enables powerful NQL queries, comprehensive KPI tracking aligned with industry standards, and sophisticated predictive analytics capabilities.

Key success factors:
1. **Data Quality First**: Maintain >95% data quality scores
2. **User Adoption**: Focus on intuitive visualizations and self-service analytics
3. **Incremental Value**: Start with core KPIs, expand to predictive analytics
4. **Continuous Improvement**: Regular model retraining and KPI refinement
5. **Cross-functional Alignment**: Ensure KPIs drive business objectives

The implementation roadmap provides a structured approach to realizing value from the data infrastructure, with clear milestones and success metrics. By following this guide, organizations can transform their manufacturing data into actionable insights that drive operational excellence and competitive advantage.