import os

filepath = 'README.md'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace trailing slashes followed by newlines and spaces that were left behind
content = content.replace('" \\\n \n', '"\n')
content = content.replace('" \\\n\n', '"\n')

# Sometimes it might just be the slash and space
content = content.replace('" \\\n', '"\n')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
