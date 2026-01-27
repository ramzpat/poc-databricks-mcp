#!/usr/bin/env python3
"""
Example usage of the create_temp_table tool for lead generation.

This example demonstrates how to use the create_temp_table tool to:
1. Create aggregated views combining multiple tables
2. Generate lead segments based on business criteria
3. Analyze campaign effectiveness across data sources

Note: This is a demonstration script. In production, this tool would be called
via the MCP protocol from an AI agent like Claude.
"""

# Example 1: Simple Lead Aggregation
example_1 = {
    "name": "High-Value Customer Summary",
    "description": "Create a temp table summarizing high-value customers",
    "temp_table_name": "high_value_customers",
    "sql_query": """
        SELECT 
            customer_id,
            customer_name,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(order_amount) as lifetime_value,
            AVG(order_amount) as avg_order_value,
            MAX(order_date) as last_order_date
        FROM `main`.`sales`.`orders`
        WHERE order_amount > 1000
            AND order_date >= '2024-01-01'
        GROUP BY customer_id, customer_name
        HAVING lifetime_value > 10000
        ORDER BY lifetime_value DESC
    """
}

# Example 2: Multi-Table Lead Generation
example_2 = {
    "name": "Qualified Lead Segments",
    "description": "Combine companies, contacts, and transactions for qualified leads",
    "temp_table_name": "qualified_leads",
    "sql_query": """
        SELECT 
            c.id as company_id,
            c.name as company_name,
            c.industry,
            c.employee_count,
            c.revenue as annual_revenue,
            COUNT(DISTINCT ct.id) as contact_count,
            COUNT(DISTINCT ct.id) FILTER (WHERE ct.job_level = 'Executive') as executive_contacts,
            COUNT(DISTINCT t.id) as transaction_count,
            SUM(t.amount) as total_transaction_value,
            AVG(t.amount) as avg_transaction_value,
            MAX(t.transaction_date) as last_transaction_date,
            CASE 
                WHEN c.revenue > 100000000 THEN 'Enterprise'
                WHEN c.revenue > 10000000 THEN 'Mid-Market'
                ELSE 'SMB'
            END as segment
        FROM `main`.`default`.`companies` c
        LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
        LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
        WHERE c.status = 'active'
            AND c.created_date >= '2023-01-01'
        GROUP BY c.id, c.name, c.industry, c.employee_count, c.revenue
        HAVING 
            contact_count > 0
            AND (total_transaction_value > 50000 OR executive_contacts > 0)
    """
}

# Example 3: Campaign Effectiveness Analysis
example_3 = {
    "name": "Campaign ROI Analysis",
    "description": "Analyze campaign effectiveness across operational and analytics data",
    "temp_table_name": "campaign_roi",
    "sql_query": """
        SELECT 
            c.id as campaign_id,
            c.campaign_name,
            c.campaign_type,
            c.start_date,
            c.end_date,
            c.budget,
            COUNT(DISTINCT l.id) as lead_count,
            COUNT(DISTINCT o.id) as conversion_count,
            CAST(COUNT(DISTINCT o.id) AS FLOAT) / NULLIF(COUNT(DISTINCT l.id), 0) * 100 as conversion_rate,
            SUM(o.amount) as revenue_generated,
            SUM(o.amount) - c.budget as net_profit,
            (SUM(o.amount) - c.budget) / NULLIF(c.budget, 0) * 100 as roi_percentage,
            a.engagement_score,
            a.click_through_rate,
            a.email_open_rate
        FROM `main`.`marketing`.`campaigns` c
        LEFT JOIN `main`.`sales`.`leads` l 
            ON c.id = l.campaign_id 
            AND l.created_date BETWEEN c.start_date AND DATEADD(day, 30, c.end_date)
        LEFT JOIN `main`.`sales`.`orders` o 
            ON l.id = o.lead_id
        LEFT JOIN `analytics`.`reporting`.`campaign_metrics` a 
            ON c.id = a.campaign_id
        WHERE c.start_date >= '2024-01-01'
        GROUP BY 
            c.id, c.campaign_name, c.campaign_type, c.start_date, c.end_date, 
            c.budget, a.engagement_score, a.click_through_rate, a.email_open_rate
        HAVING lead_count > 0
        ORDER BY roi_percentage DESC
    """
}

# Example 4: Industry-Specific Lead Scoring
example_4 = {
    "name": "Technology Sector Lead Scoring",
    "description": "Score leads in technology sector based on multiple factors",
    "temp_table_name": "tech_lead_scores",
    "sql_query": """
        SELECT 
            c.id,
            c.name,
            c.industry,
            c.sub_industry,
            -- Revenue score (0-10)
            CASE 
                WHEN c.revenue > 100000000 THEN 10
                WHEN c.revenue > 50000000 THEN 9
                WHEN c.revenue > 10000000 THEN 7
                WHEN c.revenue > 5000000 THEN 5
                WHEN c.revenue > 1000000 THEN 3
                ELSE 1
            END as revenue_score,
            -- Employee count score (0-10)
            CASE 
                WHEN c.employee_count > 10000 THEN 10
                WHEN c.employee_count > 1000 THEN 8
                WHEN c.employee_count > 500 THEN 6
                WHEN c.employee_count > 100 THEN 4
                ELSE 2
            END as size_score,
            -- Contact quality score (0-10)
            CASE 
                WHEN COUNT(DISTINCT ct.id) FILTER (WHERE ct.job_level = 'C-Level') > 2 THEN 10
                WHEN COUNT(DISTINCT ct.id) FILTER (WHERE ct.job_level = 'C-Level') > 0 THEN 8
                WHEN COUNT(DISTINCT ct.id) FILTER (WHERE ct.job_level = 'VP') > 2 THEN 7
                WHEN COUNT(DISTINCT ct.id) FILTER (WHERE ct.job_level = 'Director') > 1 THEN 5
                WHEN COUNT(DISTINCT ct.id) > 0 THEN 3
                ELSE 0
            END as contact_score,
            -- Engagement score (0-10)
            CASE 
                WHEN COUNT(DISTINCT i.id) FILTER (WHERE i.interaction_date >= CURRENT_DATE - INTERVAL 30 DAYS) > 10 THEN 10
                WHEN COUNT(DISTINCT i.id) FILTER (WHERE i.interaction_date >= CURRENT_DATE - INTERVAL 30 DAYS) > 5 THEN 7
                WHEN COUNT(DISTINCT i.id) FILTER (WHERE i.interaction_date >= CURRENT_DATE - INTERVAL 90 DAYS) > 3 THEN 5
                WHEN COUNT(DISTINCT i.id) FILTER (WHERE i.interaction_date >= CURRENT_DATE - INTERVAL 180 DAYS) > 0 THEN 3
                ELSE 1
            END as engagement_score,
            -- Transaction history score (0-10)
            CASE 
                WHEN SUM(t.amount) > 1000000 THEN 10
                WHEN SUM(t.amount) > 500000 THEN 8
                WHEN SUM(t.amount) > 100000 THEN 6
                WHEN SUM(t.amount) > 10000 THEN 4
                WHEN COUNT(t.id) > 0 THEN 2
                ELSE 0
            END as transaction_score
        FROM `main`.`default`.`companies` c
        LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
        LEFT JOIN `main`.`sales`.`interactions` i ON c.id = i.company_id
        LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
        WHERE c.industry = 'Technology'
            AND c.status = 'active'
        GROUP BY 
            c.id, c.name, c.industry, c.sub_industry, 
            c.revenue, c.employee_count
    """
}

# Example 5: Geographic Market Analysis
example_5 = {
    "name": "Regional Market Penetration",
    "description": "Analyze market penetration by geographic region",
    "temp_table_name": "regional_analysis",
    "sql_query": """
        SELECT 
            c.country,
            c.state_province,
            c.city,
            c.industry,
            COUNT(DISTINCT c.id) as company_count,
            COUNT(DISTINCT CASE WHEN c.status = 'customer' THEN c.id END) as customer_count,
            COUNT(DISTINCT CASE WHEN c.status = 'prospect' THEN c.id END) as prospect_count,
            SUM(c.revenue) as total_market_revenue,
            AVG(c.revenue) as avg_company_revenue,
            SUM(t.amount) as total_sales,
            AVG(t.amount) as avg_transaction_size,
            COUNT(DISTINCT ct.id) as total_contacts,
            COUNT(DISTINCT CASE WHEN ct.job_level IN ('C-Level', 'VP') THEN ct.id END) as decision_maker_contacts,
            -- Market penetration rate
            CAST(COUNT(DISTINCT CASE WHEN c.status = 'customer' THEN c.id END) AS FLOAT) 
                / NULLIF(COUNT(DISTINCT c.id), 0) * 100 as penetration_rate
        FROM `main`.`default`.`companies` c
        LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
        LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
        WHERE c.country IN ('USA', 'Canada', 'UK', 'Germany', 'France')
        GROUP BY c.country, c.state_province, c.city, c.industry
        HAVING company_count >= 10
        ORDER BY total_sales DESC, company_count DESC
    """
}

# Example 6: Account-Based Marketing Target List
example_6 = {
    "name": "ABM Priority Accounts",
    "description": "Create priority account list for account-based marketing",
    "temp_table_name": "abm_priority_accounts",
    "sql_query": """
        WITH company_metrics AS (
            SELECT 
                c.id,
                c.name,
                c.industry,
                c.revenue,
                c.employee_count,
                COUNT(DISTINCT ct.id) as contact_count,
                COUNT(DISTINCT ct.id) FILTER (WHERE ct.job_level IN ('C-Level', 'VP')) as decision_maker_count,
                MAX(i.interaction_date) as last_interaction_date,
                COUNT(DISTINCT i.id) FILTER (WHERE i.interaction_date >= CURRENT_DATE - INTERVAL 90 DAYS) as recent_interactions,
                SUM(t.amount) as historical_revenue,
                COUNT(DISTINCT o.id) as open_opportunities,
                SUM(o.estimated_value) as pipeline_value
            FROM `main`.`default`.`companies` c
            LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
            LEFT JOIN `main`.`sales`.`interactions` i ON c.id = i.company_id
            LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
            LEFT JOIN `main`.`sales`.`opportunities` o ON c.id = o.company_id AND o.status = 'open'
            WHERE c.status IN ('prospect', 'customer')
                AND c.revenue > 10000000
            GROUP BY c.id, c.name, c.industry, c.revenue, c.employee_count
        )
        SELECT 
            id,
            name,
            industry,
            revenue as annual_revenue,
            employee_count,
            contact_count,
            decision_maker_count,
            last_interaction_date,
            recent_interactions,
            historical_revenue,
            open_opportunities,
            pipeline_value,
            -- Priority score (0-100)
            LEAST(100, 
                (CASE WHEN revenue > 100000000 THEN 25 WHEN revenue > 50000000 THEN 20 ELSE 15 END) +
                (LEAST(25, decision_maker_count * 5)) +
                (LEAST(20, recent_interactions * 2)) +
                (CASE WHEN historical_revenue > 500000 THEN 15 WHEN historical_revenue > 100000 THEN 10 ELSE 5 END) +
                (LEAST(15, open_opportunities * 3))
            ) as priority_score,
            -- Prioritization tier
            CASE 
                WHEN decision_maker_count >= 3 AND revenue > 50000000 THEN 'Tier 1'
                WHEN decision_maker_count >= 2 AND revenue > 10000000 THEN 'Tier 2'
                WHEN decision_maker_count >= 1 OR recent_interactions > 5 THEN 'Tier 3'
                ELSE 'Nurture'
            END as priority_tier
        FROM company_metrics
        WHERE 
            (decision_maker_count > 0 OR recent_interactions > 0)
            AND contact_count > 0
        ORDER BY priority_score DESC
    """
}

def print_example(example: dict) -> None:
    """Pretty print an example query."""
    print(f"\n{'='*80}")
    print(f"Example: {example['name']}")
    print(f"{'='*80}")
    print(f"\nDescription: {example['description']}")
    print(f"\nTemp Table Name: {example['temp_table_name']}")
    print(f"\nSQL Query:")
    print("-" * 80)
    print(example['sql_query'])
    print("-" * 80)
    print(f"\nUsage via MCP:")
    print(f"""
    create_temp_table(
        temp_table_name="{example['temp_table_name']}",
        sql_query='''
            {example['sql_query'].strip()}
        '''
    )
    """)

def main():
    """Display all examples."""
    print("\n" + "="*80)
    print("CREATE_TEMP_TABLE TOOL - USAGE EXAMPLES")
    print("Lead Generation & Audience Sizing Scenarios")
    print("="*80)
    
    examples = [
        example_1,
        example_2,
        example_3,
        example_4,
        example_5,
        example_6,
    ]
    
    for example in examples:
        print_example(example)
    
    print("\n" + "="*80)
    print("IMPORTANT NOTES:")
    print("="*80)
    print("""
    1. All queries must be SELECT statements
    2. All catalogs/schemas must be in your allowlist (config.yml)
    3. Temporary views are session-scoped and auto-cleaned
    4. Focus on aggregated data for privacy-first design
    5. Use for lead generation, not raw data retrieval
    6. Queries are validated for security patterns
    7. Review CREATE_TEMP_TABLE_GUIDE.md for detailed documentation
    """)

if __name__ == "__main__":
    main()
