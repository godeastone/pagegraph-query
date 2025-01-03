import argparse
import os
import sys
from copy import deepcopy

from six.moves.urllib.parse import urlparse
from six.moves.urllib.parse import urlunparse


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--mapping_path', required=True,
                      help='Path to the mapping file')
  parser.add_argument('--html_path', required=True,
                      help='Path to the modified HTML files')
  parser.add_argument('--modified', action='store_true')
  return parser.parse_args()


def parse_csv(path):
  with open(path, 'r') as f:
    return [x.strip().split(',') for x in f.readlines()]


def write_file(path, c):
  with open(path, 'w') as f:
    f.write(c)


def get_domain_name(html_name):
  return html_name[:-5]


def get_modified_html_list(html_path):
  return [os.path.join(html_path, x) for x in os.listdir(html_path)]


def add_idx_to_url(url, idx, modified):
  parsed_url = urlparse(url)
  if modified:
    added_url = [
      parsed_url.scheme,
      parsed_url.netloc,
      os.path.join(parsed_url.path, 'idx_' + idx),
      parsed_url.params,
      parsed_url.query,
      parsed_url.fragment,
    ]
  else:
      added_url = [
      parsed_url.scheme,
      parsed_url.netloc,
      parsed_url.path,
      parsed_url.params,
      parsed_url.query,
      parsed_url.fragment,
    ]
  return urlunparse(added_url)


def remove_idx_from_path(html_path, modified):
  html_path = os.path.basename(html_path)
  html_path = html_path[:-5]
  if modified:
    idx = html_path.split('_')[-1]
    idx_removed_html_path = html_path[:-(len(idx) + 1)]
  else:
    idx = "0"
    idx_removed_html_path = html_path
  return idx, idx_removed_html_path


def create_new_entries(html_path, mapping_path):
  modified_html_list = get_modified_html_list(html_path)
  entries = parse_csv(mapping_path)
  new_entries = []
  for modified_html_path in modified_html_list:
    idx, idx_removed_html_name = remove_idx_from_path(modified_html_path, args.modified)
    for entry in entries:
      original_html_name = os.path.basename(entry[0])
      if original_html_name[:-5] == idx_removed_html_name:
        new_entry = (modified_html_path, add_idx_to_url(entry[1], idx, args.modified))
        new_entries += [new_entry]
  return new_entries


def write_new_entries(new_entries):
  c = '\n'.join([','.join(new_entry) for new_entry in new_entries])
  write_file('final_url_to_modified_html_filepath_mapping_AE.csv', c)


if __name__ == '__main__':
  args = get_args()
  new_entries = create_new_entries(args.html_path, args.mapping_path)
  write_new_entries(new_entries)
