#!/usr/bin/env python3
"""
Script to extract column information from dbt catalog.json and format for schema.yml
"""
import json
import yaml

def extract_columns_from_catalog():
    """Extract column information from catalog.json"""
    
    with open('target/catalog.json', 'r') as f:
        catalog = json.load(f)
    
    sources = catalog.get('sources', {})
    
    # Define the source tables we want to update
    source_tables = {
        'erp_next': [
            'tabMonthly_Digest',
            'tabNPS_Survey', 
            'tabCustomer',
            'tabOpportunity',
            'tabProduct_Type_Table',
            'tabFocus_Area_Table',
            'tabEmployee',
            'resource_allocation',
            'sales_invoice',
            'budget_actuals'
        ],
        'staging_dalgo': [
            'consulting_time_tracker',
            'support_issues_tracker',
            'follower_statistics',
            'share_statistics',
            'organization_lookup',
            'total_follower_count'
        ],
        'staging_zoho': [
            'campaign_recipients',
            'campaign_reports',
            'recent_campaigns',
            'mailing_lists'
        ]
    }
    
    result = {}
    
    for source_name, tables in source_tables.items():
        result[source_name] = {}
        for table in tables:
            source_key = f"source.tech4dev.{source_name}.{table}"
            if source_key in sources:
                columns = sources[source_key].get('columns', {})
                result[source_name][table] = []
                
                # Sort columns by index
                sorted_columns = sorted(columns.items(), key=lambda x: x[1].get('index', 0))
                
                for col_name, col_info in sorted_columns:
                    col_type = col_info.get('type', 'unknown')
                    result[source_name][table].append({
                        'name': col_name,
                        'type': col_type,
                        'description': f"{col_name.replace('_', ' ').title()}"
                    })
    
    return result

def format_columns_for_yaml(columns_data):
    """Format the column data for YAML insertion"""
    output = {}
    
    for source_name, tables in columns_data.items():
        output[source_name] = {}
        for table_name, columns in tables.items():
            yaml_columns = []
            for col in columns:
                col_entry = {
                    'name': col['name'],
                    'description': col['description']
                }
                # Add type for specific columns that had it in original schema
                if col['name'] in ['name', 'status', 'project', 'subject', 'priority', 'department', 'parent_task', 'project_name']:
                    if 'character varying' in col['type']:
                        col_entry['type'] = 'character varying'
                elif col['name'] in ['progress', 'actual_time', 'expected_time', 'percent_complete', 'total_hours', 'balance_hours']:
                    if 'numeric' in col['type']:
                        col_entry['type'] = col['type']
                elif col['name'] in ['exp_end_date', 'exp_start_date', 'expected_end_date', 'expected_start_date', 'month_start_date']:
                    if 'date' in col['type']:
                        col_entry['type'] = 'date'
                elif col['name'] == 'total_capacity':
                    if 'bigint' in col['type']:
                        col_entry['type'] = 'bigint'
                
                # Add config for consistency with existing schema
                col_entry['config'] = {
                    'meta': {},
                    'tags': []
                }
                yaml_columns.append(col_entry)
            
            output[source_name][table_name] = yaml_columns
    
    return output

def main():
    print("Extracting column information from catalog.json...")
    columns_data = extract_columns_from_catalog()
    formatted_data = format_columns_for_yaml(columns_data)
    
    # Print formatted output for each source
    for source_name, tables in formatted_data.items():
        print(f"\n=== {source_name.upper()} ===")
        for table_name, columns in tables.items():
            print(f"\n{table_name}:")
            print(f"  Columns found: {len(columns)}")
            for col in columns[:5]:  # Show first 5 columns as sample
                print(f"    - {col['name']} ({col.get('type', 'no type')})")
            if len(columns) > 5:
                print(f"    ... and {len(columns) - 5} more columns")

if __name__ == "__main__":
    main()
