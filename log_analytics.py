import csv
import sys
import netaddr


export_data, key_set = set(), set()


def open_file(file_name, mode='r'):
    if mode == 'w':
        file_name = 'out/' + file_name
    if '.csv' not in file_name:
        file_name += '.csv'
    return open(file_name, mode, encoding='UTF8', newline='')


input_rows = ()


def read_input_file():
    global input_rows
    file_name = sys.argv[1]
    with open_file(mode='r', file_name=file_name) as fr:
        rows = csv.reader(fr)
        header = next(rows)
        input_rows += tuple(rows)


read_input_file()


def clean_guadicore_data():
    global key_set
    for rw in input_rows:
        if len(rw) != 6:
            continue
        key_set_size = len(key_set)
        key_set.add((rw[1], rw[3], rw[4], rw[5].upper()))
        if len(key_set) > key_set_size:
            export_data.add((rw[0], rw[1], rw[2], rw[3], rw[4], rw[5].upper()))


def clean_netflow_data():
    global key_set
    for rw in input_rows:
        if len(rw) == 6:
            continue
        key_set_size = len(key_set)
        rw3_upper = rw[3].upper()
        rw3, rw4 = rw3_upper.split('/')[0], rw3_upper.split('/')[1]
        key_set.add((rw[0], rw[2], rw3, rw4))
        if len(key_set) > key_set_size:
            export_data.add(('', rw[0], '', rw[2], rw3, rw4))


export_data = input_rows

def address_is_ipv6(addr):
    return 6 in (netaddr.IPNetwork(addr).version, netaddr.IPAddress(addr).version)


def address_in_subnet(address, subnet):
    return netaddr.IPAddress(address) in netaddr.IPNetwork(subnet)


def address_in_subnets(address, subnets):
    for subnet in subnets:
        if address_in_subnet(address, subnet):
            return True
    return False


def map_from_address_to_pool_name(address):
    if address_is_ipv6(address):
        return 'IPV6'
    name_to_subnets = {
        'CDC': ('10.118.0.0/16',),
        'VPN': ('10.254.112.0/21', '10.254.120.0/21'),
        'PL': ('10.3.0.0/16',),
        'OLD_AWS': ('10.254.128.0/17',),
        'NEW_AWS': ('44.118.128.0/17',)}
    for name, subnets in name_to_subnets.items():
        if address_in_subnets(address, subnets):
            return name
    return 'UNKNOWN'


def add_cols(my_row):
    my_row_as_list = list(my_row)
    from_col = map_from_address_to_pool_name(my_row[1])
    to_col = map_from_address_to_pool_name(my_row[3])
    my_row_as_list.append(from_col)
    my_row_as_list.append(to_col)
    return tuple(my_row_as_list)


def adjust_columns(rows):
    adjusted_rows = []
    for my_row in rows:
        adjusted_rows.append(add_cols(my_row))
    return adjusted_rows


file_contents = {
                    'UNKNOWN': [], 'IPV6': [],
                    'CDC_CDC': [], 'CDC_VPN': [],
                    'CDC_PL': [], 'CDC_OLD_AWS': [], 'CDC_NEW_AWS': [],
                    'VPN_PL': [], 'VPN_OLD_AWS': [], 'VPN_NEW_AWS': [],
                    'PL_OLD_AWS': [], 'PL_NEW_AWS': [], }

header = ('source_asset_name', 'source_ip', 'destination_asset_name',
          'destination_ip', 'destination_port', 'ip_protocol', 'from', 'to')

export_data = adjust_columns(export_data)


def resolve_unknown_or_ipv6(name_parts, row):
    target_content = 'UNKNOWN' if 'UNKNOWN' in name_parts else 'IPV6'
    file_contents[target_content].append(row)


def two_diffrent_pools_and_both_in_file_name(name_parts, file_name):
    two_diff_pools = name_parts[0] != name_parts[1]
    both_in_fname = name_parts[0] in file_name and name_parts[1] in file_name
    return two_diff_pools and both_in_fname


def same_pool(name_parts):
    return name_parts[0] == name_parts[1]


def set_file_contents():
    global export_data, file_contents
    for row in export_data:
        for file_name in file_contents:
            name_parts = row[-2], row[-1]
            if 'UNKNOWN' in name_parts or 'IPV6' in name_parts:
                resolve_unknown_or_ipv6(name_parts, row)
                break
            elif two_diffrent_pools_and_both_in_file_name(name_parts, file_name):
                file_contents[file_name].append(row)
                break
            elif same_pool(name_parts):
                file_name = name_parts[0] + '_' + name_parts[0]
                file_contents[file_name].append(row)
                break
            else:
                pass


set_file_contents()

def write_output_files_by_pool():
    for file_name in file_contents:
        with open_file(mode='w', file_name=file_name) as fw:
            writer = csv.writer(fw)
            writer.writerow(header)
            writer.writerows(file_contents[file_name])


write_output_files_by_pool()


def write_main_output_file():
    with open_file(mode='w', file_name=sys.argv[-1]) as fw:
        writer = csv.writer(fw)
        writer.writerow(header)
        writer.writerows(export_data)