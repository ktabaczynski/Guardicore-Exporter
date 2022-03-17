import csv
import sys


export_data = set()

def open_file(arg_pos, mode='r'):
    return open(sys.argv[arg_pos], mode, encoding='UTF8', newline='')


for i in range(1, len(sys.argv)-1):
    with open_file(mode='r', arg_pos=i) as fr:        
        csvreader = csv.reader(fr)
        header = next(csvreader)
        for row in csvreader:
            if len(row) > 24:
                source_asset_name, src_ip, dest_asset_name, dest_ip, dest_port, dest_proto = row[7], row[5], row[16], row[14], row[23], row[24]
            else:
                source_asset_name, src_ip, dest_asset_name, dest_ip, dest_port, dest_proto = row[0], row[1], row[2], row[3], row[4], row[5]            
            export_data.add((source_asset_name, src_ip, dest_asset_name, dest_ip, dest_port, dest_proto))

with open_file(mode='w', arg_pos=-1) as fw:
    writer = csv.writer(fw)
    writer.writerow( ('source_asset_name', 'source_ip', 'destination_asset_name', 'destination_ip', 'destination_port', 'ip_protocol') )
    for row in export_data:
        writer.writerow(row)