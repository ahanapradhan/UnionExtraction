import re


def parse_sql_query(sql_query):
    # Initialize lists to store grouping and ordering attributes
    group_by_attributes = []
    order_by_attributes = []

    # Regular expressions to match GROUP BY and ORDER BY clauses
    group_by_pattern = re.compile(r'\bGROUP BY\b\s*(.+?)(?=\b(?:ORDER BY|LIMIT|;)\b|$)', re.IGNORECASE | re.DOTALL)
    order_by_pattern = re.compile(r'\bORDER BY\b\s*(.+?)(?=\b(?:LIMIT|;)\b|$)', re.IGNORECASE | re.DOTALL)

    # Check for GROUP BY clause
    group_by_match = group_by_pattern.search(sql_query)
    if group_by_match:
        group_by_attributes = [attr.strip() for attr in group_by_match.group(1).split(',')]

    # Check for ORDER BY clause
    order_by_match = order_by_pattern.search(sql_query)
    if order_by_match:
        order_by_attributes = [attr.strip() for attr in order_by_match.group(1).split(',')]

    return group_by_attributes, order_by_attributes
