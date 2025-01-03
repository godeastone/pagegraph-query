import argparse
import json
import os
import re
from collections import deque
from multiprocessing import Process
from time import time

import networkx as nx
import pagegraph.commands
import tldextract
from six.moves.urllib.parse import urlparse
from six.moves.urllib.parse import urlunparse
from pagegraph.graph.node import Node


def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--graph_dir', required=True,
                      help='Path to the PageGraph data')
  parser.add_argument('--feature_dir', required=True,
                      help='Path to save the extracted features')
  parser.add_argument('--mapping_path', required=True,
                      help='Path to the mapping file')
  parser.add_argument('-j', type=int, required=True,
                      help='# of jobs')
  parser.add_argument('-t', type=int, required=True,
                      help='Timeout in seconds')
  parser.add_argument('--modified', action='store_true',
                      help='Path to the mapping file')
  return parser.parse_args()


def read_file(fpath):
  with open(fpath, 'r') as f:
    return [x.strip() for x in f.readlines()]


def write_features(features, graph_path):
  os.makedirs(feature_dir, exist_ok=True)
  feature_name = os.path.basename(graph_path)[:-8] + '.json'
  feature_path = os.path.join(feature_dir, feature_name)
  features = json.dumps(features, indent=4)
  with open(feature_path, 'w') as f:
    f.write(features)


def get_graph_path(graph_dir):
  graph_paths = []
  for domain in os.listdir(graph_dir):
    for fname in os.listdir(os.path.join(graph_dir, domain)):
      if fname.endswith('.graphml'):
        graph_path = os.path.join(graph_dir, domain, fname)
        feature_name = os.path.basename(graph_path)[:-8] + '.json'
        feature_path = os.path.join(feature_dir, feature_name)
        if (
          os.path.getsize(graph_path) != 0 and
          not os.path.exists(feature_path)
        ):
          graph_paths += [graph_path]

  return graph_paths


def get_target_url(graph_path):
  for l in read_file(mapping_path):
    target_url = l.split(',')[1]
    html_name = os.path.basename(l.split(',')[0])[:-5]
    graph_name = os.path.basename(graph_path)[:-8]
    if html_name == graph_name:
      return target_url


def remove_url_idx(url):
  parsed_url = urlparse(url)
  path = os.path.dirname(parsed_url.path)
  if path != '' and path[-1] != '/':
    path += '/'
  removed_url = [
    parsed_url.scheme,
    parsed_url.netloc,
    path,
    parsed_url.params,
    parsed_url.query,
    parsed_url.fragment
  ]
  return urlunparse(removed_url)


def get_requester_node(pg, request_report):
  request_start_edge = get_request_start_edge(pg, request_report)
  return request_start_edge.incoming_node()


def get_parents(pg, request_report):
  requester_node = get_requester_node(pg, request_report)
  return list(requester_node.parent_nodes())


def get_children(pg, request_report):
  requester_node = get_requester_node(pg, request_report)
  return list(requester_node.child_nodes())


def get_node_type(node):
  return node.node_type()


def get_request_start_edge(pg, request_report):
  return pg.edge(request_report.request.request.id)


def get_request_result_edge(pg, request_report):
  if request_report.request.result != None:
    return pg.edge(request_report.request.result.id)
  else:
    return None


def get_request_url(request_report):
  return request_report.request.request.url


def get_timestap_from_edge(edge):
  return edge.timestamp()


def get_start_time(pg, request_report):
  request_start_edge = get_request_start_edge(pg, request_report)
  return get_timestap_from_edge(request_start_edge)


def get_complete_time(pg, request_report):
  request_result_edge = get_request_result_edge(pg, request_report)
  if request_result_edge != None:
    return get_timestap_from_edge(request_result_edge)
  else:
    return None


def get_etld_plus_one(url):
  extracted = tldextract.extract(url)
  etld_plus_one = f'{extracted.domain}.{extracted.suffix}'
  return etld_plus_one


def extract_url_length(pg, target_url, request_report, graph_path):
  request_url = get_request_url(request_report)
  return len(request_url)


def extract_from_subdomain(pg, target_url, request_report, graph_path):
  request_url = get_request_url(request_report)
  return (
    get_etld_plus_one(target_url) ==
    get_etld_plus_one(request_url)
  )


def extract_from_third_party(pg, target_url, request_report, graph_path):
  request_url = get_request_url(request_report)
  return (
    get_etld_plus_one(target_url) !=
    get_etld_plus_one(request_url)
  )


def extract_semicolon_in_query(pg, target_url, request_report, graph_path):
  request_url = get_request_url(request_report)
  parsed_url = urlparse(request_url)
  return ';' in parsed_url.query


# NUM_CLASS = 30
def extract_resource_type(pg, target_url, request_report, graph_path):
  return str(request_report.request.request_type)


def extract_load_time(pg, target_url, request_report, graph_path):
  start_time = get_start_time(pg, request_report)
  end_time = get_complete_time(pg, request_report)
  if end_time != None:
    return end_time - start_time
  else:
    return 0


def extract_in_degree(pg, target_url, request_report, graph_path):
  requester_node = get_requester_node(pg, request_report)
  return len(list(requester_node.incoming_edges()))


def extract_out_degree(pg, target_url, request_report, graph_path):
  requester_node = get_requester_node(pg, request_report)
  return len(list(requester_node.outgoing_edges()))


def extract_in_out_degree(pg, target_url, request_report, graph_path):
  return (
    extract_in_degree(pg, target_url, request_report, graph_path) +
    extract_out_degree(pg, target_url, request_report, graph_path)
  )


def extract_modified_by_script(pg, target_url, request_report, graph_path):
  requester_node = get_requester_node(pg, request_report)
  for parent_node in get_parents(pg, request_report):
    if get_node_type(parent_node) == Node.Types.SCRIPT:
      for outgoing_edge in parent_node.outgoing_edges():
        if outgoing_edge.outgoing_node() == requester_node:
          return True
  return False


def extract_parent_in_degree(pg, target_url, request_report, graph_path):
  parent_in_degree = 0
  for parent_node in get_parents(pg, request_report):
    parent_in_degree += len(list(parent_node.incoming_edges()))
  return parent_in_degree


def extract_parent_out_degree(pg, target_url, request_report, graph_path):
  parent_out_degree = 0
  for parent_node in get_parents(pg, request_report):
    parent_out_degree += len(list(parent_node.outgoing_edges()))
  return parent_out_degree


def extract_parent_in_out_degree(pg, target_url, request_report, graph_path):
  return (
    extract_parent_in_degree(pg, target_url, request_report, graph_path) +
    extract_parent_out_degree(pg, target_url, request_report, graph_path)
  )


def extract_parent_modified_by_script(
  pg, target_url, request_report, graph_path
):
  for parent_node in get_parents(pg, request_report):
    for grandparent_node in parent_node.parent_nodes():
      if get_node_type(grandparent_node) == Node.Types.SCRIPT:
        for outgoing_edge in grandparent_node.outgoing_edges():
          if outgoing_edge.outgoing_node() == parent_node:
            return True
  return False


def extract_avg_degree_connectivity(
  pg, target_url, request_report, graph_path
):
  in_out_degree = extract_in_out_degree(
    pg, target_url, request_report, graph_path
  )
  return nx.average_degree_connectivity(pg.graph)[in_out_degree]


FEATURE_FUNC_MAP = {
  'FEATURE_URL_LENGTH': extract_url_length,
  'FEATURE_FROM_SUBDOMAIN': extract_from_subdomain,
  'FEATURE_FROM_THIRD_PARTY': extract_from_third_party,
  'FEATURE_SEMICOLON_IN_QUERY': extract_semicolon_in_query,
  'FEATURE_RESOURCE_TYPE': extract_resource_type,
  'FEATURE_LOAD_TIME': extract_load_time,
  'FEATURE_IN_DEGREE': extract_in_degree,
  'FEATURE_OUT_DEGREE': extract_out_degree,
  'FEATURE_IN_OUT_DEGREE': extract_in_out_degree,
  'FEATURE_MODIFIED_BY_SCRIPT': extract_modified_by_script,
  'FEATURE_PARENT_IN_DEGREE': extract_parent_in_degree,
  'FEATURE_PARENT_OUT_DEGREE': extract_parent_out_degree,
  'FEATURE_PARENT_IN_OUT_DEGREE': extract_parent_in_out_degree,
  'FEATURE_PARENT_MODIFIED_BY_SCRIPT': extract_parent_modified_by_script,
  'FEATURE_AVERAGE_DEGREE_CONNECTIVITY': extract_avg_degree_connectivity,
}


def extract_features(pg, target_url, request_report, graph_path):
  features = {}
  for name, func in FEATURE_FUNC_MAP.items():
    features[name] = func(pg, target_url, request_report, graph_path)
  return features


def extract_request_features(graph_path):
  print(graph_path)

  # Preprocess the target URL
  target_url = get_target_url(graph_path)
  if is_modified:
    target_url = remove_url_idx(target_url)

  pg, requests = pagegraph.commands.requests(graph_path, None, False)
  request_features = []
  for request_report in requests:
    features = extract_features(pg, target_url, request_report, graph_path)

    request_url = get_request_url(request_report)
    features['NETWORK_REQUEST_URL'] = request_url
    features['FINAL_URL'] = target_url

    request_features += [features]
  write_features(request_features, graph_path)


def extract_in_parallel(graph_queue, timeout, num_jobs):
  proc_list = []
  while len(graph_queue) > 0:
    # Start feature extraction in parallel
    if len(proc_list) < num_jobs:
      graph_path = graph_queue.popleft()
      proc = Process(target=extract_request_features, args=(graph_path,))
      proc.start()
      start_time = time()
      proc_list += [(proc, graph_path, start_time)]
    # Wait for the processes
    else:
      for proc, graph_path, start_time in proc_list[:]:
        if not proc.is_alive():
          proc_list.remove((proc, graph_path, start_time))
        else:
          cur_time = time()
          if cur_time - start_time > timeout:
            print('[!] Feature extraction time out (%s)' % graph_path)
            proc.terminate()
            proc.join()
            proc_list.remove((proc, graph_path, start_time))

  while len(proc_list) > 0:
    for proc, graph_path, start_time in proc_list[:]:
      if not proc.is_alive():
        proc_list.remove((proc, graph_path, start_time))
      else:
        cur_time = time()
        if cur_time - start_time > timeout:
          print('[!] Feature extraction time out (%s)' % graph_path)
          proc.terminate()
          proc.join()
          proc_list.remove((proc, graph_path, start_time))


if __name__ == '__main__':
  args = get_args()
  mapping_path = args.mapping_path
  feature_dir = args.feature_dir
  is_modified = args.modified
  num_jobs = args.j
  timeout = args.t
  graph_paths = get_graph_path(args.graph_dir)
  graph_queue = deque(graph_paths)
  extract_in_parallel(graph_queue, timeout, num_jobs)
