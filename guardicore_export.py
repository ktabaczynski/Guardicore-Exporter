import csv
import sys

data=[]
export_data=set()


count=0
with open(sys.argv[1]) as fp:
    while True:
        count += 1        
        line = fp.readline()
        if count==1:
            continue
        if not line:
            break
        double_quotes_separated=line.split('""')
        src_ip=double_quotes_separated[0].split(',')[5]

        source_asset_name=double_quotes_separated[0].split(',')[7]
        dest_asset_name=double_quotes_separated[4].split(',')[6]

        dest_ip=double_quotes_separated[4].split(',')[4]
        dest_port,dest_proto=double_quotes_separated[8].split(',')[4],double_quotes_separated[8].split(',')[5].strip()
        data.append([src_ip,dest_ip,dest_port,dest_proto])

        export_data.add((source_asset_name,src_ip,dest_asset_name,dest_ip,dest_port,dest_proto))

with open(sys.argv[2], 'w', encoding='UTF8',newline='') as f:
    writer = csv.writer(f)
    for row in export_data:
      writer.writerow(row)      