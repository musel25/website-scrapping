import csv

input_file = 'tijuana_no_website.csv'
output_file = 'places_with_phone.csv'

with open(input_file, newline='', encoding='utf-8') as infile, \
    open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in reader:
       phone = row.get('phone') or row.get('Phone') or row.get('telefono') or row.get('Telefono')
       if phone and phone.strip():
          writer.writerow(row)