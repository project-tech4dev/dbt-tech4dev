#!/usr/bin/env python3
"""
Script to generate YAML column definitions from dbt catalog.json
"""
import json

def get_columns_for_table(source_name, table_name):
    """Get formatted column list for a specific table"""
    
    with open('target/catalog.json', 'r') as f:
        catalog = json.load(f)
    
    sources = catalog.get('sources', {})
    source_key = f"source.tech4dev.{source_name}.{table_name}"
    
    if source_key not in sources:
        return f"# Table {source_key} not found in catalog"
    
    columns = sources[source_key].get('columns', {})
    
    # Sort columns by index
    sorted_columns = sorted(columns.items(), key=lambda x: x[1].get('index', 0))
    
    yaml_lines = []
    yaml_lines.append("        columns:")
    
    for col_name, col_info in sorted_columns:
        col_type = col_info.get('type', 'unknown')
        
        # Generate description based on column name
        description = generate_description(col_name)
        
        yaml_lines.append(f"          - name: {col_name}")
        
        # Add type for relevant columns
        if should_include_type(col_name, col_type):
            yaml_lines.append(f"            type: {col_type}")
            
        yaml_lines.append(f"            description: {description}")
        yaml_lines.append("            config:")
        yaml_lines.append("              meta: {}")
        yaml_lines.append("              tags: []")
    
    return "\n".join(yaml_lines)

def generate_description(col_name):
    """Generate meaningful descriptions for columns"""
    
    # Special cases for common column patterns
    descriptions = {
        'idx': 'Index of the record',
        'name': 'Unique identifier or name',
        '_seen': 'Indicator if the record has been seen',
        'owner': 'Owner of the record',
        '_assign': 'Assignment details',
        'creation': 'Creation timestamp',
        'modified': 'Last modified timestamp',
        '_comments': 'Comments on the record',
        '_liked_by': 'Users who liked the record',
        'docstatus': 'Document status',
        '_user_tags': 'User tags associated with the record',
        'modified_by': 'User who last modified the record',
        'project': 'Associated project identifier',
        'subject': 'Subject or title',
        'status': 'Current status',
        'priority': 'Priority level',
        'progress': 'Progress percentage',
        'description': 'Description',
        'department': 'Department name',
        'employee': 'Employee identifier',
        'employee_name': 'Employee name',
        '_airbyte_raw_id': 'Airbyte raw ID',
        '_airbyte_extracted_at': 'Airbyte extraction timestamp',
        '_airbyte_meta': 'Airbyte metadata',
        'product_type': 'Product type',
        'customer': 'Customer identifier',
        'company': 'Company identifier',
        'email': 'Email address',
        'phone': 'Phone number',
        'city': 'City',
        'state': 'State',
        'country': 'Country',
        'campaign_id': 'Campaign identifier',
        'campaign_key': 'Campaign key',
        'campaign_name': 'Campaign name',
        'sent_time': 'Time when sent',
        'contactid': 'Contact identifier',
        'contactemailaddress': 'Contact email address'
    }
    
    if col_name in descriptions:
        return descriptions[col_name]
    
    # Pattern-based descriptions
    if col_name.endswith('_date'):
        return f"{col_name.replace('_', ' ').title().replace('Date', 'date')}"
    elif col_name.endswith('_time'):
        return f"{col_name.replace('_', ' ').title().replace('Time', 'time')}"
    elif col_name.endswith('_count'):
        return f"Number of {col_name.replace('_count', '').replace('_', ' ')}"
    elif col_name.endswith('_id'):
        return f"Identifier for {col_name.replace('_id', '').replace('_', ' ')}"
    elif col_name.startswith('total_'):
        return f"Total {col_name.replace('total_', '').replace('_', ' ')}"
    elif col_name.startswith('custom_'):
        return f"Custom {col_name.replace('custom_', '').replace('_', ' ')}"
    elif col_name.startswith('is_'):
        return f"Indicates if {col_name.replace('is_', '').replace('_', ' ')}"
    elif col_name.startswith('has_'):
        return f"Indicates if has {col_name.replace('has_', '').replace('_', ' ')}"
    elif 'percent' in col_name or 'rate' in col_name:
        return f"{col_name.replace('_', ' ').title()} percentage"
    else:
        return col_name.replace('_', ' ').title()

def should_include_type(col_name, col_type):
    """Determine if we should include the type in the YAML"""
    
    # Include type for specific important columns
    type_columns = [
        'name', 'status', 'project', 'subject', 'priority', 'department', 
        'parent_task', 'project_name', 'employee', 'month', 'customer'
    ]
    
    # Include type for date columns
    if col_name in type_columns and 'character varying' in col_type:
        return True
    elif col_name.endswith('_date') and 'date' in col_type:
        return True
    elif col_name in ['progress', 'actual_time', 'expected_time', 'percent_complete', 'total_hours', 'balance_hours'] and 'numeric' in col_type:
        return True
    elif col_name == 'total_capacity' and 'bigint' in col_type:
        return True
    elif col_name == 'year' and 'bigint' in col_type:
        return True
    
    return False

def main():
    """Generate column definitions for each source table"""
    
    # Tables that need columns filled
    tables_to_update = [
        ('erp_next', 'tabMonthly_Digest'),
        ('erp_next', 'tabNPS_Survey'),
        ('erp_next', 'tabCustomer'),
        ('erp_next', 'tabOpportunity'),
        ('erp_next', 'tabProduct_Type_Table'),
        ('erp_next', 'tabFocus_Area_Table'),
        ('erp_next', 'tabEmployee'),
        ('erp_next', 'resource_allocation'),
        ('erp_next', 'sales_invoice'),
        ('erp_next', 'budget_actuals'),
        ('staging_dalgo', 'consulting_time_tracker'),
        ('staging_dalgo', 'support_issues_tracker'),
        ('staging_dalgo', 'follower_statistics'),
        ('staging_dalgo', 'share_statistics'),
        ('staging_dalgo', 'organization_lookup'),
        ('staging_dalgo', 'total_follower_count'),
        ('staging_zoho', 'campaign_recipients'),
        ('staging_zoho', 'campaign_reports'),
        ('staging_zoho', 'recent_campaigns'),
        ('staging_zoho', 'mailing_lists')
    ]
    
    for source_name, table_name in tables_to_update:
        print(f"\n=== {source_name}.{table_name} ===")
        columns_yaml = get_columns_for_table(source_name, table_name)
        print(columns_yaml)

if __name__ == "__main__":
    main()
