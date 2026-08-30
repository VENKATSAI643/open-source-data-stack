import os

filepath = 'README.md'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace all occurrences of the password flag
content = content.replace(' -u "default:ch_admin_2025"', '')
content = content.replace('-u "default:ch_admin_2025"', '')

# Also replace the password in the connection strings if it exists
content = content.replace('default:ch_admin_2025@', 'default@')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
