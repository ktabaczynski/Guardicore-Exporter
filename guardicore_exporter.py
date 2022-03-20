import csv
import sys


export_data=set()

def open_file(mode='r'):
    arg_pos= 2 if mode=='w' else 1
    return open(sys.argv[arg_pos],mode,encoding='UTF8',newline='')

with open_file('r') as fr:
    csvreader = csv.reader(fr)
    for row in csvreader:
      source_asset_name,src_ip,dest_asset_name,dest_ip,dest_port,dest_proto=row[7],row[5],row[16],row[14],row[23],row[24]
      export_data.add((source_asset_name,src_ip,dest_asset_name,dest_ip,dest_port,dest_proto))

with open_file('w') as fw:
    writer = csv.writer(fw)
    writer.writerow( ('source_asset_name', 'source_ip', 'destination_asset_name', 'destination_ip','destination_port','ip_protocol') )
    for row in export_data:
      writer.writerow(row)