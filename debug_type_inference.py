#!/usr/bin/env python3
"""
Debug script to test type inference for datetime values.
"""

from splurge_tools.type_helper import profile_values, DataType as ToolsDataType

# Test values from the sample data
test_values = [
    "2024-01-15 09:30:00",
    "2024-01-14 14:45:00", 
    "2024-01-10 16:20:00",
    "2024-01-16 08:15:00",
    "2024-01-13 11:30:00"
]

print("Testing type inference for datetime values:")
print(f"Values: {test_values}")
print()

# Test the profile_values function
result = profile_values(test_values)
print(f"profile_values result: {result}")
print(f"Result type: {type(result)}")

# Test individual values
print("\nTesting individual values:")
for value in test_values:
    individual_result = profile_values([value])
    print(f"'{value}' -> {individual_result}") 